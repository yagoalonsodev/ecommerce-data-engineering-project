"""
Logging centralizado: formato con timestamp y pipeline step.
Uso: setup_logging() al inicio; get_step_logger(module, step) en cada paso del pipeline.
"""

import logging
import sys
from pathlib import Path

# Formato: timestamp | level | pipeline_step | message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Nombres de loggers por pipeline / setup
PANDAS_ETL_LOGGER = "pandas_etl"
PYSPARK_ETL_LOGGER = "pyspark_etl"
LOAD_NEON_LOGGER = "load_neon_db"


def setup_logging(
    level: int = logging.INFO,
    log_file: Path | str | None = None,
    log_to_console: bool = True,
) -> None:
    """
    Configura el logging global: formato con timestamp y nombre (step).
    Si log_file está definido, escribe también a ese archivo.
    """
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    root = logging.getLogger()

    # Evitar duplicar handlers si se llama varias veces
    if root.handlers:
        return

    root.setLevel(level)

    if log_to_console:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        root.addHandler(console)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


def get_step_logger(module: str, step: str) -> logging.Logger:
    """
    Devuelve un logger con nombre 'module.step' para que en el formato
    aparezca el pipeline step (ej: pandas_etl.load_data, pyspark_etl.transform_data).
    """
    return logging.getLogger(f"{module}.{step}")
