"""
Tests mínimos del ETL PySpark: sesión creada, columnas clave en el DataFrame.
No se ejecuta el pipeline completo ni se escribe en Neon.
"""
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_CSV = PROJECT_ROOT / "tests" / "fixtures" / "sample_ecommerce_raw.csv"
# CSV con columnas normalizadas (order_id, order_date) para transform_data
FIXTURE_NORMALIZED_CSV = PROJECT_ROOT / "tests" / "fixtures" / "sample_ecommerce_normalized.csv"

# Columnas que debe tener el dataset tras transformaciones (fact/dim)
EXPECTED_COLUMNS_RAW = ["order_id", "order_date", "customer_id", "product_id", "quantity", "total_amount"]


def test_spark_session_creates():
    """La sesión Spark se crea correctamente."""
    from data_engineering.etl_pipelines.pyspark_etl import create_spark_session
    spark = create_spark_session()
    assert spark is not None
    spark.stop()


def test_load_raw_data_returns_dataframe_with_columns():
    """load_raw_data devuelve un DataFrame con columnas esperadas."""
    from data_engineering.etl_pipelines.pyspark_etl import create_spark_session, load_raw_data
    spark = create_spark_session()
    try:
        df = load_raw_data(spark, str(FIXTURE_CSV))
        assert df is not None
        assert df.count() > 0, "El DataFrame no debe estar vacío"
        cols_lower = [c.lower().replace(" ", "_") for c in df.columns]
        for expected in ["order_id", "product_id", "customer_id", "order_date", "quantity"]:
            assert any(expected in c or c == expected for c in cols_lower), f"Se esperaba columna relacionada con {expected}"
    finally:
        spark.stop()


def test_transform_data_preserves_key_columns():
    """Tras transform_data, las columnas clave siguen presentes."""
    from data_engineering.etl_pipelines.pyspark_etl import (
        create_spark_session,
        load_raw_data,
        transform_data,
    )
    spark = create_spark_session()
    try:
        # Usar CSV con columnas ya normalizadas (order_date, etc.)
        df = load_raw_data(spark, str(FIXTURE_NORMALIZED_CSV))
        df = transform_data(df)
        cols = [c.lower() for c in df.columns]
        assert "order_id" in cols or any("order" in c for c in cols)
        assert "product_id" in cols or any("product" in c for c in cols)
        assert df.count() > 0
    finally:
        spark.stop()
