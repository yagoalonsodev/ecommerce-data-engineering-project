"""
Pipeline PySpark: transformaciones escalables y modelo dimensional.
Lee datos limpios de Pandas (data/processed/) y genera fact_sales, dim_products, dim_customers, dim_date.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Crea o obtiene la sesión Spark para el ETL de eCommerce."""
    return (
        SparkSession.builder.appName("Ecommerce ETL")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def load_raw_data(spark: SparkSession, path: str | Path) -> DataFrame:
    """Carga el CSV procesado (salida de pandas_etl) con cabecera e inferencia de schema."""
    path = str(Path(path))
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info("Datos cargados desde %s: %s filas", path, df.count())
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """
    Transformaciones ligeras en Spark. Los datos ya vienen limpios de Pandas;
    aquí solo aseguramos tipos o columnas necesarias para el modelo dimensional.
    """
    # Asegurar que order_date sea fecha para dim_date y fact
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
    # Dim Products: una fila por product_id
    dim_products = (
        df.select("product_id", "product_name", "category", "subcategory", "product_brand")
        .distinct()
        .orderBy("product_id")
    )
    logger.info("dim_products: %s filas", dim_products.count())

    # Dim Customers: una fila por customer_id
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
    logger.info("dim_customers: %s filas", dim_customers.count())

    # Dim Date: una fila por fecha distinta (order_date), con atributos temporales
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
    logger.info("dim_date: %s filas", dim_date.count())

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
    base_path: str | Path = "warehouse",
    format: str = "parquet",
    connection_url: Optional[str] = None,
) -> None:
    """
    Escribe las tablas en el warehouse.
    Si connection_url está definido, usa JDBC (NeonDB); si no, escribe en base_path como Parquet/CSV.
    """
    base_path = Path(base_path)
    base_path.mkdir(parents=True, exist_ok=True)

    if connection_url:
        for name, table_df in tables.items():
            table_df.write.format("jdbc").option("url", connection_url).option(
                "dbtable", name
            ).mode("overwrite").save()
            logger.info("Escrito en warehouse (JDBC): %s", name)
    else:
        for name, table_df in tables.items():
            out_path = base_path / name
            table_df.write.mode("overwrite").format(format).save(str(out_path))
            logger.info("Escrito en %s (%s)", out_path, format)


def run_pipeline(
    input_path: str | Path = "data/processed/ecommerce_clean.csv",
    warehouse_path: str | Path = "warehouse",
    connection_url: Optional[str] = None,
) -> dict[str, Any]:
    """
    Ejecuta el pipeline completo PySpark:
    load -> transform -> build_dimensional_tables -> build_fact_tables -> write_to_warehouse.
    Devuelve un diccionario con los DataFrames generados (para pruebas o inspección).
    """
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
    logger.info("Pipeline PySpark completado")
    return all_tables


if __name__ == "__main__":
    run_pipeline()
