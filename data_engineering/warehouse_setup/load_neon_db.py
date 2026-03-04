"""
Crea el schema del warehouse en NeonDB (Fase 2).
Ejecuta schema_star.sql usando DATABASE_URL. Ejecutar ANTES de la primera escritura con PySpark.
Sigue las mismas prácticas que los pipelines: config centralizada, logging por pasos, métricas.
"""

import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.logging_config import LOAD_NEON_LOGGER, get_step_logger, setup_logging
from config.observability import log_pipeline_metrics
from config.settings import Settings

# Ruta al schema (constante del módulo)
SCHEMA_FILE = Path(__file__).resolve().parent.parent / "warehouse_schemas" / "schema_star.sql"


def run_schema(connection_url: str, schema_path: Path = SCHEMA_FILE) -> int:
    """
    Ejecuta el SQL del schema contra la base de datos.
    Devuelve el número de sentencias ejecutadas (para métricas).
    """
    logger = get_step_logger(LOAD_NEON_LOGGER, "run_schema")
    if not schema_path.exists():
        logger.error("Schema no encontrado: %s", schema_path)
        raise FileNotFoundError(f"Schema no encontrado: {schema_path}")

    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 no instalado. Ejecuta: pip install psycopg2-binary")
        raise ImportError("Instala psycopg2-binary: pip install psycopg2-binary") from None

    sql_content = schema_path.read_text(encoding="utf-8")
    statements = [
        s.strip()
        for s in sql_content.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]
    logger.info("Aplicando schema desde %s (%s sentencias)", schema_path, len(statements))

    conn = psycopg2.connect(connection_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                if stmt:
                    cur.execute(stmt)
        logger.info("Schema aplicado correctamente (%s sentencias) en NeonDB", len(statements))
        return len(statements)
    except Exception as e:
        logger.error("Error aplicando schema: %s", e)
        raise
    finally:
        conn.close()


def main() -> None:
    """Punto de entrada: valida config, aplica schema y registra métricas."""
    setup_logging(
        log_file=Settings.LOG_DIR / "load_neon_db.log",
        log_to_console=True,
    )
    logger = get_step_logger(LOAD_NEON_LOGGER, "main")
    start = time.perf_counter()

    try:
        Settings.validate_db_config()
        db_url = Settings.get_database_url()
        if not db_url:
            logger.error("DATABASE_URL o DB_HOST+DB_NAME no configurados. Revisa .env")
            raise ValueError(
                "DATABASE_URL o DB_HOST+DB_NAME no configurados. Revisa .env"
            )

        num_statements = run_schema(db_url)
        duration = time.perf_counter() - start
        log_pipeline_metrics(
            pipeline_name="load_neon_db",
            duration_seconds=duration,
            rows_processed=0,
            status="success",
            extra={"statements": num_statements},
        )
        logger.info("load_neon_db completado en %.2f s", duration)
    except Exception as e:
        duration = time.perf_counter() - start
        log_pipeline_metrics(
            pipeline_name="load_neon_db",
            duration_seconds=duration,
            rows_processed=0,
            status="failure",
            extra={"error": str(e)},
        )
        logger.error("load_neon_db falló: %s", e)
        raise


if __name__ == "__main__":
    main()
