"""
Tests mínimos del ETL Pandas: dataframe no vacío, columnas clave, sin nulls en PK.
"""
import pandas as pd
import pytest
from pathlib import Path

# Fixture path (relativo a raíz del proyecto)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_CSV = PROJECT_ROOT / "tests" / "fixtures" / "sample_ecommerce_raw.csv"

KEY_COLUMNS = ["order_id", "product_id", "customer_id", "order_date", "quantity", "total_amount"]
PK_COLUMNS = ["order_id", "product_id"]


def test_pandas_etl_output_not_empty():
    """El pipeline no debe devolver un dataframe vacío."""
    from data_engineering.etl_pipelines.pandas_etl import run_pipeline
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        out_path = Path(f.name)
    try:
        df = run_pipeline(input_path=FIXTURE_CSV, output_path=out_path)
        assert df is not None
        assert len(df) > 0, "El dataframe de salida no debe estar vacío"
    finally:
        if out_path.exists():
            out_path.unlink()


def test_pandas_etl_output_has_key_columns():
    """El output debe tener las columnas clave para el modelo dimensional."""
    from data_engineering.etl_pipelines.pandas_etl import run_pipeline
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        out_path = Path(f.name)
    try:
        df = run_pipeline(input_path=FIXTURE_CSV, output_path=out_path)
        for col in KEY_COLUMNS:
            assert col in df.columns, f"Falta columna clave: {col}"
    finally:
        if out_path.exists():
            out_path.unlink()


def test_pandas_etl_output_no_nulls_in_pk():
    """No debe haber nulls en columnas que actúan como PK (order_id, product_id)."""
    from data_engineering.etl_pipelines.pandas_etl import run_pipeline
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        out_path = Path(f.name)
    try:
        df = run_pipeline(input_path=FIXTURE_CSV, output_path=out_path)
        for col in PK_COLUMNS:
            assert col in df.columns
            null_count = df[col].isna().sum()
            assert null_count == 0, f"Hay {null_count} nulls en {col}"
    finally:
        if out_path.exists():
            out_path.unlink()


def test_pandas_etl_saved_csv_exists_and_valid():
    """El CSV guardado debe existir y contener datos válidos."""
    from data_engineering.etl_pipelines.pandas_etl import run_pipeline
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        out_path = Path(f.name)
    try:
        run_pipeline(input_path=FIXTURE_CSV, output_path=out_path)
        assert out_path.exists()
        saved = pd.read_csv(out_path)
        assert len(saved) > 0
        assert "order_id" in saved.columns
        assert saved["order_id"].notna().all()
    finally:
        if out_path.exists():
            out_path.unlink()
