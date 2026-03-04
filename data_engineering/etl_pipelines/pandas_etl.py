"""
Pipeline Pandas: exploración, limpieza y preparación inicial.
No genera el warehouse final; guarda dataset limpio en data/processed/.
"""

import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from config.logging_config import PANDAS_ETL_LOGGER, get_step_logger, setup_logging
from config.observability import log_pipeline_metrics
from config.settings import Settings


def load_data(path: str | Path) -> pd.DataFrame:
    """Carga el dataset desde CSV."""
    logger = get_step_logger(PANDAS_ETL_LOGGER, "load_data")
    path = Path(path)
    if not path.exists():
        logger.error("Archivo no encontrado: %s", path)
        raise FileNotFoundError(f"No se encuentra el archivo: {path}")
    df = pd.read_csv(path)
    logger.info("Datos cargados: %s filas, %s columnas", len(df), len(df.columns))
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpieza general: nombres de columnas en minúsculas y con guión bajo."""
    logger = get_step_logger(PANDAS_ETL_LOGGER, "clean_data")
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    logger.info("Columnas normalizadas: %s", list(df.columns))
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas sin order_id o product_id.
    Filtra cantidades <= 0.
    Rellena numéricos con mediana y discount con 0.
    """
    logger = get_step_logger(PANDAS_ETL_LOGGER, "handle_missing_values")
    df = df.copy()
    initial = len(df)

    df = df.dropna(subset=["order_id", "product_id"])
    df = df[df["quantity"] > 0]

    if "unit_price" in df.columns:
        df["unit_price"] = df["unit_price"].fillna(df["unit_price"].median())
        df = df[df["unit_price"] >= 0]
    if "total_amount" in df.columns:
        df["total_amount"] = df["total_amount"].fillna(
            df["quantity"] * df["unit_price"]
        )
    if "discount" in df.columns:
        df["discount"] = df["discount"].fillna(0)
    if "customer_age" in df.columns:
        df["customer_age"] = df["customer_age"].fillna(df["customer_age"].median())

    logger.info("Missing values: %s -> %s filas", initial, len(df))
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza fechas (múltiples formatos) y customer_gender (Male/Female/Other).
    Rellena categorías y textos vacíos con 'Unknown'. Elimina filas sin order_date.
    """
    logger = get_step_logger(PANDAS_ETL_LOGGER, "normalize_columns")
    df = df.copy()

    # Fechas
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(
            df["order_date"], format="mixed", dayfirst=True, errors="coerce"
        )
        df = df.dropna(subset=["order_date"])
    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(
            df["delivery_date"], format="mixed", dayfirst=True, errors="coerce"
        )
        df["delivery_date"] = df["delivery_date"].fillna(df["order_date"])

    # Género
    if "customer_gender" in df.columns:
        genero_map = {"m": "Male", "male": "Male", "f": "Female", "female": "Female"}
        s = df["customer_gender"].astype(str).str.strip().str.lower()
        df["customer_gender"] = s.map(lambda x: genero_map.get(x, "Other"))
        df["customer_gender"] = df["customer_gender"].replace("nan", "Other")

    # Categorías y texto
    for col in (
        "category", "subcategory", "product_brand", "payment_method", "order_status",
        "customer_id", "customer_name", "country", "city", "customer_email",
        "product_name", "shipping_address",
    ):
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str).str.strip()
            df.loc[df[col] == "", col] = "Unknown"

    logger.info("Columnas normalizadas (fechas, género, categorías)")
    return df


def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Feature engineering básico eCommerce: total_revenue, métricas de tiempo.
    Ajusta tipos numéricos: quantity entero; unit_price, total_amount, discount, total_revenue con 2 decimales.
    """
    logger = get_step_logger(PANDAS_ETL_LOGGER, "feature_engineering")
    df = df.copy()

    df["total_revenue"] = df["quantity"] * df["unit_price"]

    if "order_date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["order_date"]):
        df["order_year"] = df["order_date"].dt.year
        df["order_month"] = df["order_date"].dt.month
        df["order_year_month"] = df["order_date"].dt.to_period("M").astype(str)

    # Tipos y decimales: quantity entero; precios y % con 2 decimales
    df["quantity"] = df["quantity"].astype(int)
    for col in ("unit_price", "total_amount", "discount", "total_revenue"):
        if col in df.columns:
            df[col] = df[col].round(2)

    logger.info("Feature engineering aplicado: total_revenue, order_year, order_month; numéricos formateados")
    return df


def validate_data_quality(df: pd.DataFrame) -> bool:
    """Valida que no queden nulos y que cantidades/precios sean coherentes."""
    logger = get_step_logger(PANDAS_ETL_LOGGER, "validate_data_quality")
    try:
        assert df.isnull().sum().sum() == 0, "Quedan valores nulos en el dataset"
        assert (df["quantity"] > 0).all(), "Todas las cantidades deben ser positivas"
        assert (df["unit_price"] >= 0).all(), "Los precios no pueden ser negativos"
    except AssertionError as e:
        logger.error("Validación fallida: %s", e)
        raise
    logger.info("Validación de calidad: OK")
    return True


def save_processed_data(df: pd.DataFrame, path: str | Path) -> None:
    """Guarda el dataset limpio en CSV (sin índice)."""
    logger = get_step_logger(PANDAS_ETL_LOGGER, "save_processed_data")
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info("Datos guardados en %s (%s filas)", path, len(df))
    except OSError as e:
        logger.error("Error al guardar %s: %s", path, e)
        raise


def run_pipeline(
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Ejecuta el pipeline completo: load -> clean -> missing -> normalize -> features -> validate -> save.
    Paths por defecto desde Settings (PATH_RAW_CSV, PATH_PROCESSED_CSV).
    """
    setup_logging(log_file=Settings.LOG_DIR / "pandas_etl.log", log_to_console=True)
    run_logger = get_step_logger(PANDAS_ETL_LOGGER, "run_pipeline")

    input_path = Path(input_path) if input_path is not None else Settings.PATH_RAW_CSV
    output_path = Path(output_path) if output_path is not None else Settings.PATH_PROCESSED_CSV

    run_logger.info("Inicio pipeline Pandas: %s -> %s", input_path, output_path)
    start_time = time.time()
    rows_in = 0
    try:
        df = load_data(input_path)
        rows_in = len(df)
        df = clean_data(df)
        df = handle_missing_values(df)
        df = normalize_columns(df)
        df = feature_engineering(df)
        validate_data_quality(df)
        save_processed_data(df, output_path)
        rows_out = len(df)
        duration = time.time() - start_time
        run_logger.info(
            "Pipeline Pandas completado en %.2f s | Rows: input=%s, output=%s",
            duration, rows_in, rows_out,
        )
        log_pipeline_metrics(
            "pandas_etl", duration, rows_out, "success",
            extra={"rows_in": rows_in},
        )
        return df
    except Exception as e:
        duration = time.time() - start_time
        run_logger.error("Pipeline Pandas fallido tras %.2f s: %s", duration, e)
        log_pipeline_metrics(
            "pandas_etl", duration, rows_in, "failure",
            extra={"error": str(e)},
        )
        raise


if __name__ == "__main__":
    run_pipeline()
