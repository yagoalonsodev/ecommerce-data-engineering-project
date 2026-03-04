"""
Capa de acceso a datos. Usa Settings.get_database_url(); no hardcodear conexión.
Route → Service → DB (arquitectura limpia).
"""

from typing import Any, Optional

from config.settings import Settings


def get_connection():
    """Devuelve una conexión a la DB usando la URL de config (Neon)."""
    url = Settings.get_database_url()
    if not url:
        raise ValueError("DATABASE_URL no configurada. Revisa .env")
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        raise ImportError("Instala psycopg2-binary: pip install psycopg2-binary") from None
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def run_query(query: str, params: Optional[tuple | dict] = None) -> list[dict[str, Any]]:
    """
    Ejecuta una consulta SELECT y devuelve filas como list[dict].
    Uso desde services: analytics_service llama a run_query(...).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()
    finally:
        conn.close()
