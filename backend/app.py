"""
Aplicación Flask. Usa config/settings (Settings.get_database_url()); no hardcodea DB.
Arquitectura: Route → Service → DB.
"""

from flask import Flask

from backend.routes import analytics, customers, products, sales


def create_app() -> Flask:
    app = Flask(__name__)
    app.register_blueprint(analytics.bp)
    app.register_blueprint(sales.bp)
    app.register_blueprint(products.bp)
    app.register_blueprint(customers.bp)

    @app.get("/")
    def index():
        return {"service": "ecommerce-analytics-api", "docs": "/analytics/kpis"}

    return app


app = create_app()
