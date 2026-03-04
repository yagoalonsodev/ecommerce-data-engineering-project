# Ecommerce Data Engineering Project

[![CI](https://github.com/yagoalonsodev/ecommerce-data-engineering-project/actions/workflows/ci.yml/badge.svg)](https://github.com/yagoalonsodev/ecommerce-data-engineering-project/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-150458?logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Neon-336791?logo=postgresql&logoColor=white)](https://neon.tech/)
[![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)](https://react.dev/)
[![Vite](https://img.shields.io/badge/Vite-5-646CFF?logo=vite&logoColor=white)](https://vitejs.dev/)
[![Tailwind CSS](https://img.shields.io/badge/Tailwind-3-38BDF8?logo=tailwindcss&logoColor=black)](https://tailwindcss.com/)
[![Recharts](https://img.shields.io/badge/Recharts-2-8884D8)](https://recharts.org/)
[![pytest](https://img.shields.io/badge/pytest-7+-0A9EDC?logo=pytest&logoColor=white)](https://pytest.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Netlify](https://img.shields.io/badge/Netlify-Frontend-00C7B7?logo=netlify&logoColor=white)](https://www.netlify.com/)
[![Vercel](https://img.shields.io/badge/Vercel-Backend-000000?logo=vercel&logoColor=white)](https://vercel.com/)

Plataforma de ingeniería de datos end-to-end para análisis eCommerce: pipelines ETL con Pandas y PySpark, modelo dimensional en PostgreSQL (Neon), API REST con Flask, dashboard en React, tests con cobertura y CI/CD.

---

## Stack

| Área | Tecnologías |
|------|-------------|
| **Datos** | Pandas, PySpark, Neon (PostgreSQL) |
| **Backend** | Flask, Flask-CORS, psycopg2, Route → Service → DB |
| **Frontend** | React 18, Vite 5, Tailwind CSS, Recharts |
| **Tests** | pytest, pytest-cov, GitHub Actions |
| **Deploy** | Netlify (frontend), Vercel (backend API) |

---

## Enlaces

- **Dashboard (producción):** [ecommerce-etl.netlify.app](https://ecommerce-etl.netlify.app)
- **API (producción):** [ecommerce-data-engineering-project-chi.vercel.app](https://ecommerce-data-engineering-project-chi.vercel.app)

---

## 🏗 Arquitectura

<p align="center">
  <img src="docs/architecture.png" alt="Diagrama de arquitectura" width="900"/>
</p>

```
Raw CSV (~10k registros con inconsistencias)
        │
        ▼
ETL Layer
  - Pandas (limpieza rápida, validaciones)
  - PySpark (transformación escalable)
        │
        ▼
Data Warehouse
  - Neon (PostgreSQL)
  - Modelo estrella (fact_sales + tablas dim)
        │
        ▼
Backend API
  - Flask
  - Arquitectura Route → Service → DB
  - Validación de parámetros
  - Manejo de errores (400, 404, 500)
  - Endpoint /health
        │
        ▼
Frontend Dashboard
  - React + Vite
  - Tailwind CSS
  - Recharts
  - Consumo de API REST
```

**Production Deployment:**
- Frontend → Netlify
- Backend → Vercel
- Database → Neon (PostgreSQL)

---

## Decisiones técnicas

| Decisión | Motivo |
|----------|--------|
| **Pandas y PySpark** | Pandas para limpieza rápida y validaciones; PySpark para simular un entorno distribuido y escalable. |
| **Neon (PostgreSQL)** | Base relacional adecuada para modelo dimensional; serverless y fácil de desplegar en producción. |
| **Route → Service → DB** | Separación de responsabilidades, facilita testing con mocks y escalado a microservicios. |
| **Tests sin base real** | Tests rápidos, CI independiente del entorno y aislamiento de la lógica de negocio. |

---

## Limitaciones actuales

- No hay autenticación (API pública).
- No hay paginación real.
- No hay rate limiting.
- No hay caching.
- El ETL no está orquestado automáticamente (se ejecuta manualmente).

---

## Métricas del proyecto

- ~10.000 registros procesados
- 2 pipelines ETL (Pandas + PySpark)
- 6 endpoints REST (incl. `/health`)
- 37+ tests automatizados
- CI con cobertura (pytest-cov)
- Deploy en producción (Netlify + Vercel)

---

## Cómo arrancar en local

### Con Docker

```bash
docker-compose up -d ecommerce-etl frontend
docker-compose exec -d ecommerce-etl bash -c "cd /app && PYTHONPATH=. flask --app backend.app run --host=0.0.0.0 --port=5000"
```

- **Dashboard:** http://localhost:3000  
- **API:** http://localhost:5001  

### Sin Docker (desarrollo)

**Backend:**

```bash
pip install -r requirements.txt
export DATABASE_URL="postgresql://..."   # o usa .env
PYTHONPATH=. flask --app backend.app run --port=5001
```

**Frontend:**

```bash
cd frontend
npm install
npm run dev
```

Abre http://localhost:5173 (el front usa `VITE_API_BASE` o fallback a `http://localhost:5001` en dev).

---

## Tests

```bash
pip install -r requirements.txt
PYTHONPATH=. pytest backend/tests tests/etl -v --cov=backend --cov=data_engineering --cov-report=term-missing
```

---

## Deploy

### Frontend (Netlify)

- **Base directory:** `frontend`
- **Build command:** `npm run build`
- **Publish directory:** `dist`
- **Variable de entorno:** `VITE_API_BASE` = URL del backend (ej. `https://tu-proyecto.vercel.app`), sin barra final.

### Backend (Vercel)

- **Root directory:** `backend`
- **Install command:** `pip install -r requirements.txt`
- **Variables de entorno:**
  - `DATABASE_URL` = URL de conexión Neon (PostgreSQL).
  - `FRONTEND_ORIGIN` = URL del frontend en Netlify (ej. `https://tu-app.netlify.app`) para CORS.

---

## Estructura del proyecto

```
├── backend/          # API Flask (rutas, servicios, DB)
├── config/           # Settings, variables de entorno
├── data_engineering/ # ETL Pandas + PySpark
├── frontend/         # React + Vite + Tailwind + Recharts
├── tests/           # Tests ETL y backend
├── docker-compose.yml
└── requirements.txt  # Dependencias Python (raíz)
```

---

## Licencia

MIT
