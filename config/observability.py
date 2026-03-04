"""
Observabilidad básica: métricas de pipeline (execution time, rows processed, success/failure).
Cada ejecución escribe una línea en pipeline_metrics.log para poder calcular success rate.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from config.settings import Settings


def log_pipeline_metrics(
    pipeline_name: str,
    duration_seconds: float,
    rows_processed: int | dict[str, int],
    status: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """
    Registra una ejecución del pipeline para observabilidad.
    status: "success" | "failure"
    rows_processed: total (int) o dict con filas por tabla (PySpark).
    """
    log_path = Settings.LOG_DIR / "pipeline_metrics.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = (
        rows_processed
        if isinstance(rows_processed, int)
        else sum(rows_processed.values())
    )
    record = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "pipeline": pipeline_name,
        "duration_sec": round(duration_seconds, 2),
        "rows_processed": total_rows,
        "rows_detail": rows_processed if isinstance(rows_processed, dict) else None,
        "status": status,
        **(extra or {}),
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
