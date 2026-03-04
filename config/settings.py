"""
Configuración centralizada: paths, URLs, credentials.
Patrón Config Class (enterprise). Validación de variables requeridas.
No hardcodear valores sensibles; todo desde variables de entorno o defaults seguros.
"""

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

_ROOT = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass
# Fallback: cargar .env manualmente si existe (sin python-dotenv)
_env_path = _ROOT / ".env"
if _env_path.exists():
    for _ln in _env_path.read_text(encoding="utf-8").splitlines():
        _ln = _ln.strip()
        if _ln and not _ln.startswith("#") and "=" in _ln:
            _key, _, _val = _ln.partition("=")
            _key, _val = _key.strip(), _val.strip().strip("'\"")
            if _key and _key not in os.environ:
                os.environ[_key] = _val


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _path_env(key: str, default: Path) -> Path:
    return Path(_env(key) or str(default))


def _parse_pg_url(url: str) -> dict:
    """Extrae user, password, host, port, dbname de una URL postgresql:// (ej. Neon)."""
    if not url or not url.startswith("postgresql"):
        return {}
    p = urlparse(url)
    if not p.netloc or not p.path:
        return {}
    # netloc = user:password@host:port
    userinfo, _, hostport = p.netloc.rpartition("@")
    if not userinfo or not hostport:
        return {}
    user, _, password = userinfo.partition(":")
    if ":" in hostport:
        host, port = hostport.rsplit(":", 1)
    else:
        host, port = hostport, "5432"
    dbname = p.path.lstrip("/").split("?")[0]
    return {"user": user, "password": password, "host": host, "port": port, "dbname": dbname}


class Settings:
    """
    Configuración centralizada. Uso: Settings.PATH_RAW_CSV, Settings.get_jdbc_url(), etc.
    Validación: Settings.validate_required_env_vars(["DB_HOST", "DB_NAME"]) antes de usar DB.
    """
    # Raíz del proyecto
    PROJECT_ROOT: Path = _ROOT

    # ----- Paths -----
    DATA_DIR: Path = _path_env("DATA_DIR", _ROOT / "data")
    DATA_RAW_DIR: Path = _path_env("DATA_RAW_DIR", _ROOT / "data" / "raw")
    DATA_PROCESSED_DIR: Path = _path_env("DATA_PROCESSED_DIR", _ROOT / "data" / "processed")

    RAW_CSV_FILENAME: str = _env("RAW_CSV_FILENAME") or "ecommerce_raw.csv"
    PROCESSED_CSV_FILENAME: str = _env("PROCESSED_CSV_FILENAME") or "ecommerce_clean.csv"

    PATH_RAW_CSV: Path = _path_env("PATH_RAW_CSV", DATA_DIR / RAW_CSV_FILENAME)
    PATH_PROCESSED_CSV: Path = _path_env(
        "PATH_PROCESSED_CSV", DATA_PROCESSED_DIR / PROCESSED_CSV_FILENAME
    )

    WAREHOUSE_DIR: Path = _path_env("WAREHOUSE_DIR", _ROOT / "warehouse")
    LOG_DIR: Path = _path_env("LOG_DIR", _ROOT / "logs")

    # ----- URLs -----
    DATABASE_URL: str = _env("DATABASE_URL")
    DATABASE_JDBC_URL: str = _env("DATABASE_JDBC_URL")
    API_BASE_URL: str = _env("API_BASE_URL")

    # ----- Credentials (solo desde env) -----
    DB_USER: str = _env("DB_USER")
    DB_PASSWORD: str = _env("DB_PASSWORD")
    DB_HOST: str = _env("DB_HOST")
    DB_PORT: str = _env("DB_PORT") or "5432"
    DB_NAME: str = _env("DB_NAME")
    API_KEY: str = _env("API_KEY")

    @classmethod
    def get_database_url(cls) -> str:
        """URL de conexión a la base de datos (SQLAlchemy, etc.)."""
        if cls.DATABASE_URL:
            return cls.DATABASE_URL
        if cls.DB_HOST and cls.DB_NAME:
            user = cls.DB_USER or ""
            password = f":{cls.DB_PASSWORD}" if cls.DB_PASSWORD else ""
            auth = f"{user}{password}@" if user or password else ""
            return f"postgresql://{auth}{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        return ""

    @classmethod
    def get_jdbc_url(cls) -> str:
        """URL JDBC para PySpark (write_to_warehouse). Neon: añade ?sslmode=require."""
        if cls.DATABASE_JDBC_URL:
            return cls.DATABASE_JDBC_URL
        parsed = _parse_pg_url(cls.DATABASE_URL) if cls.DATABASE_URL else {}
        if parsed:
            return (
                f"jdbc:postgresql://{parsed['host']}:{parsed['port']}/{parsed['dbname']}"
                "?sslmode=require"
            )
        if cls.DB_HOST and cls.DB_NAME:
            return f"jdbc:postgresql://{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}?sslmode=require"
        return ""

    @classmethod
    def get_jdbc_user(cls) -> str:
        """Usuario para JDBC (PySpark). Desde DATABASE_URL o DB_USER."""
        if cls.DATABASE_URL:
            parsed = _parse_pg_url(cls.DATABASE_URL)
            if parsed.get("user"):
                return parsed["user"]
        return cls.DB_USER or ""

    @classmethod
    def get_jdbc_password(cls) -> str:
        """Contraseña para JDBC (PySpark). Desde DATABASE_URL o DB_PASSWORD."""
        if cls.DATABASE_URL:
            parsed = _parse_pg_url(cls.DATABASE_URL)
            if "password" in parsed:
                return parsed["password"]
        return cls.DB_PASSWORD or ""

    # Variables requeridas cuando se usa DB con host/name por separado (Fase 2)
    REQUIRED_DB_ENV_VARS: list[str] = ["DB_HOST", "DB_NAME"]

    @staticmethod
    def validate_required_env_vars(required: list[str]) -> None:
        """
        Valida que las variables de entorno requeridas estén definidas y no vacías.
        Si falta alguna, lanza ValueError (fail-fast, estándar en entornos enterprise).
        Uso: Settings.validate_required_env_vars(["DB_HOST", "DB_NAME"]) antes de conectar a DB.
        """
        missing = [var for var in required if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required env variable(s): {', '.join(missing)}")

    @classmethod
    def validate_db_config(cls) -> None:
        """
        Valida que la config de DB esté lista antes de cualquier operación con la base (Fase 2).
        Acepta: DATABASE_URL (Neon), DATABASE_JDBC_URL, o bien DB_HOST y DB_NAME definidos.
        """
        if cls.DATABASE_URL:
            return
        if cls.DATABASE_JDBC_URL:
            return
        if cls.DB_HOST and cls.DB_NAME:
            return
        raise ValueError(
            "Missing DB config for phase 2: set DATABASE_URL, DATABASE_JDBC_URL, or both DB_HOST and DB_NAME"
        )


@lru_cache(maxsize=1)
def get_settings() -> type[Settings]:
    """
    Punto de acceso único a la config; cacheado para no recalcular paths/URLs.
    Uso: get_settings().PATH_RAW_CSV, get_settings().get_jdbc_url(), etc.
    """
    return Settings


# Alias para compatibilidad con imports directos (opcional)
PATH_RAW_CSV = Settings.PATH_RAW_CSV
PATH_PROCESSED_CSV = Settings.PATH_PROCESSED_CSV
WAREHOUSE_DIR = Settings.WAREHOUSE_DIR
LOG_DIR = Settings.LOG_DIR
get_database_url = Settings.get_database_url
get_jdbc_url = Settings.get_jdbc_url
validate_required_env_vars = Settings.validate_required_env_vars
validate_db_config = Settings.validate_db_config
