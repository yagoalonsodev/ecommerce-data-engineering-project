#!/usr/bin/env bash
# Flujo ETL completo: Pandas → schema Neon → PySpark (para ejecutar dentro del contenedor o en local)
# Uso en Docker: docker-compose exec ecommerce-etl bash run_full_etl.sh
set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export PYTHONPATH=.

echo "=== 1. ETL Pandas (limpieza) ==="
python3 -m data_engineering.etl_pipelines.pandas_etl

echo "=== 2. Schema Neon (load_neon_db) ==="
python3 -m data_engineering.warehouse_setup.load_neon_db

echo "=== 3. ETL PySpark (warehouse) ==="
python3 -m data_engineering.etl_pipelines.pyspark_etl

echo "=== Flujo completo OK ==="
