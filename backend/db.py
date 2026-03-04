"""
Capa de acceso a datos. Usa Settings.get_database_url(); no hardcodear conexión.
Route → Service → DB (arquitectura limpia).
En Vercel (solo carpeta backend) usa os.environ["DATABASE_URL"] si config no existe.
"""

import os
from typing import Any, Optional

def _get_database_url() -> str:
    """URL de la DB: desde config si existe, si no desde DATABASE_URL (deploy Vercel)."""
    try:
        from config.settings import Settings
        return Settings.get_database_url() or ""
    except ImportError:
        return os.environ.get("DATABASE_URL") or ""


def get_connection():
    """Devuelve una conexión a la DB usando la URL de config (Neon)."""
    url = _get_database_url()
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
