# Ecommerce Data Engineering Project

[![CI](https://github.com/yagoalonsodev/ecommerce-data-engineering-project/actions/workflows/ci.yml/badge.svg)](https://github.com/yagoalonsodev/ecommerce-data-engineering-project/actions/workflows/ci.yml)

Plataforma de análisis eCommerce: ETL (Pandas + PySpark), warehouse en NeonDB, API Flask y dashboard React.

## Stack

- **Datos:** Pandas, PySpark, NeonDB (PostgreSQL)
- **Backend:** Flask, Route → Service → DB
- **Frontend:** React (Vite), Tailwind, Recharts
- **Tests:** pytest, pytest-cov, CI con GitHub Actions

## Cómo arrancar

```bash
# ETL + API + Frontend con Docker
docker-compose up -d ecommerce-etl frontend
docker-compose exec -d ecommerce-etl bash -c "cd /app && PYTHONPATH=. flask --app backend.app run --host=0.0.0.0 --port=5000"
```

- **Dashboard:** http://localhost:3000  
- **API:** http://localhost:5001  

## Tests

```bash
pip install -r requirements.txt
PYTHONPATH=. pytest backend/tests tests/etl -v --cov=backend --cov=data_engineering --cov-report=term-missing
```
