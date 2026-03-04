"""
Pipeline PySpark: transformaciones escalables y modelo dimensional.
Lee datos limpios de Pandas (data/processed/) y genera fact_sales, dim_products, dim_customers, dim_date.
"""

import logging
import time
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.logging_config import PYSPARK_ETL_LOGGER, get_step_logger, setup_logging
from config.observability import log_pipeline_metrics
from config.settings import Settings


def create_spark_session() -> SparkSession:
    """Crea o obtiene la sesión Spark para el ETL de eCommerce. Incluye driver JDBC PostgreSQL (Neon)."""
    logger = get_step_logger(PYSPARK_ETL_LOGGER, "create_spark_session")
    builder = (
        SparkSession.builder.appName("Ecommerce ETL")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    # Driver PostgreSQL para write_to_warehouse con NeonDB (Fase 2)
    try:
        builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    except Exception:
        pass
    spark = builder.getOrCreate()
    logger.info("Sesión Spark creada")
    return spark


def load_raw_data(spark: SparkSession, path: str | Path) -> DataFrame:
    """Carga el CSV procesado (salida de pandas_etl) con cabecera e inferencia de schema."""
    logger = get_step_logger(PYSPARK_ETL_LOGGER, "load_raw_data")
    path = str(Path(path))
    df = spark.read.csv(path, header=True, inferSchema=True)
    count = df.count()
    logger.info("Datos cargados desde %s: %s filas", path, count)
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """
    Transformaciones ligeras en Spark. Los datos ya vienen limpios de Pandas;
    aquí solo aseguramos tipos o columnas necesarias para el modelo dimensional.
    """
    logger = get_step_logger(PYSPARK_ETL_LOGGER, "transform_data")
    df = df.withColumn("order_date", F.to_date(F.col("order_date")))
    if "delivery_date" in df.columns:
        df = df.withColumn("delivery_date", F.to_date(F.col("delivery_date")))
    logger.info("Transformaciones aplicadas (fechas)")
    return df


def build_dimensional_tables(df: DataFrame) -> dict[str, DataFrame]:
    """
    Construye dim_products, dim_customers y dim_date a partir del dataset limpio.
    Devuelve un diccionario {nombre_tabla: DataFrame}.
    """
    logger_products = get_step_logger(PYSPARK_ETL_LOGGER, "build_dim_products")
    logger_customers = get_step_logger(PYSPARK_ETL_LOGGER, "build_dim_customers")
    logger_date = get_step_logger(PYSPARK_ETL_LOGGER, "build_dim_date")

    dim_products = (
        df.select("product_id", "product_name", "category", "subcategory", "product_brand")
        .distinct()
        .orderBy("product_id")
    )
    logger_products.info("dim_products: %s filas", dim_products.count())

    dim_customers = (
        df.select(
            "customer_id",
            "customer_name",
            "customer_gender",
            "customer_age",
            "country",
            "city",
            "customer_email",
        )
        .distinct()
        .orderBy("customer_id")
    )
    logger_customers.info("dim_customers: %s filas", dim_customers.count())

    dim_date = (
        df.select("order_date")
        .distinct()
        .withColumn("date_key", F.col("order_date"))
        .withColumn("year", F.year(F.col("order_date")))
        .withColumn("month", F.month(F.col("order_date")))
        .withColumn("day", F.dayofmonth(F.col("order_date")))
        .withColumn("week_of_year", F.weekofyear(F.col("order_date")))
        .withColumn("quarter", F.quarter(F.col("order_date")))
        .select("date_key", "year", "month", "day", "week_of_year", "quarter")
        .orderBy("date_key")
    )
    logger_date.info("dim_date: %s filas", dim_date.count())

    return {
        "dim_products": dim_products,
        "dim_customers": dim_customers,
        "dim_date": dim_date,
    }


def build_fact_tables(df: DataFrame) -> dict[str, DataFrame]:
    """
    Construye fact_sales: una fila por línea de pedido (order_id + product_id)
    con claves foráneas y métricas.
    """
    logger = get_step_logger(PYSPARK_ETL_LOGGER, "build_fact_tables")
    fact_sales = df.select(
        "order_id",
        "customer_id",
        "product_id",
        F.col("order_date").alias("date_key"),
        "quantity",
        "unit_price",
        "total_amount",
        "discount",
        "total_revenue",
        "payment_method",
        "order_status",
        "order_year",
        "order_month",
    ).orderBy("order_id", "product_id")
    logger.info("fact_sales: %s filas", fact_sales.count())
    return {"fact_sales": fact_sales}


def write_to_warehouse(
    tables: dict[str, DataFrame],
    base_path: str | Path,
    format: str = "parquet",
    connection_url: Optional[str] = None,
) -> None:
    """
    Escribe las tablas en el warehouse.
    Si connection_url está definido, usa JDBC (NeonDB); si no, escribe en base_path como Parquet/CSV.
    """
    logger = get_step_logger(PYSPARK_ETL_LOGGER, "write_to_warehouse")
    base_path = Path(base_path)
    base_path.mkdir(parents=True, exist_ok=True)

    if connection_url:
        Settings.validate_db_config()
        jdbc_user = Settings.get_jdbc_user()
        jdbc_password = Settings.get_jdbc_password()
        for name, table_df in tables.items():
            try:
                writer = (
                    table_df.write.format("jdbc")
                    .option("url", connection_url)
                    .option("dbtable", name)
                    .option("user", jdbc_user)
                    .option("password", jdbc_password)
                    .option("driver", "org.postgresql.Driver")
                    .mode("overwrite")
                )
                writer.save()
                logger.info("Escrito en warehouse (JDBC): %s", name)
            except Exception as e:
                logger.error("Error JDBC al escribir %s: %s", name, e)
                raise
    else:
        for name, table_df in tables.items():
            out_path = base_path / name
            table_df.write.mode("overwrite").format(format).save(str(out_path))
            logger.info("Escrito en %s (%s)", out_path, format)


def run_pipeline(
    input_path: str | Path | None = None,
    warehouse_path: str | Path | None = None,
    connection_url: Optional[str] = None,
) -> dict[str, Any]:
    """
    Ejecuta el pipeline completo PySpark:
    load -> transform -> build_dimensional_tables -> build_fact_tables -> write_to_warehouse.
    Paths por defecto desde Settings. connection_url desde Settings.get_jdbc_url() si no se pasa.
    """
    setup_logging(log_file=Settings.LOG_DIR / "pyspark_etl.log", log_to_console=True)
    run_logger = get_step_logger(PYSPARK_ETL_LOGGER, "run_pipeline")

    input_path = Path(input_path) if input_path is not None else Settings.PATH_PROCESSED_CSV
    warehouse_path = Path(warehouse_path) if warehouse_path is not None else Settings.WAREHOUSE_DIR
    if connection_url is None:
        connection_url = Settings.get_jdbc_url() or None
    if connection_url:
        Settings.validate_db_config()

    run_logger.info("Inicio pipeline PySpark: %s -> %s", input_path, warehouse_path or "JDBC")
    start_time = time.time()
    try:
        spark = create_spark_session()
        df = load_raw_data(spark, input_path)
        df = transform_data(df)

        dims = build_dimensional_tables(df)
        facts = build_fact_tables(df)
        all_tables = {**dims, **facts}

        write_to_warehouse(
            all_tables,
            base_path=warehouse_path,
            format="parquet",
            connection_url=connection_url,
        )
        rows_per_table = {name: t.count() for name, t in all_tables.items()}
        total_rows = sum(rows_per_table.values())
        duration = time.time() - start_time
        run_logger.info(
            "Pipeline PySpark completado en %.2f s | Rows processed: %s total (%s)",
            duration, total_rows, rows_per_table,
        )
        log_pipeline_metrics(
            "pyspark_etl", duration, rows_per_table, "success",
        )
        return all_tables
    except Exception as e:
        duration = time.time() - start_time
        run_logger.error("Pipeline PySpark fallido tras %.2f s: %s", duration, e)
        log_pipeline_metrics(
            "pyspark_etl", duration, 0, "failure",
            extra={"error": str(e)},
        )
        raise


if __name__ == "__main__":
    run_pipeline()
