"""
Aplicación Flask. Usa config/settings (Settings.get_database_url()); no hardcodea DB.
Arquitectura: Route → Service → DB. CORS habilitado para frontend (local + Netlify).
Imports compatibles con repo root (backend.*) y con Vercel Root=backend (routes, services, db).
"""

import os
from flask import Flask
from flask_cors import CORS

try:
    from backend.routes import analytics, customers, products, sales
except ImportError:
    from routes import analytics, customers, products, sales


def create_app() -> Flask:
    app = Flask(__name__)
    origins = ["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:5173"]
    if os.environ.get("FRONTEND_ORIGIN"):
        origins.append(os.environ["FRONTEND_ORIGIN"].rstrip("/"))
    CORS(app, origins=origins, supports_credentials=False)
    app.register_blueprint(analytics.bp)
    app.register_blueprint(sales.bp)
    app.register_blueprint(products.bp)
    app.register_blueprint(customers.bp)

    @app.get("/")
    def index():
        return {"service": "ecommerce-analytics-api", "docs": "/analytics/kpis"}

    return app


app = create_app()
