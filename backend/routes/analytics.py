"""
Rutas de analytics. Route → Service → DB (arquitectura limpia).
"""

from flask import Blueprint, jsonify

import backend.services.analytics_service as analytics_service

bp = Blueprint("analytics", __name__, url_prefix="/analytics")


@bp.get("/kpis")
def get_kpis():
    """GET /analytics/kpis — total_sales, total_revenue, total_orders, average_order_value."""
    return jsonify(analytics_service.get_kpis())  # Route → Service → DB


@bp.get("/sales-by-day")
def sales_by_day():
    """Placeholder: ventas por día (a implementar)."""
    return jsonify([])


@bp.get("/top-products")
def top_products():
    """Placeholder: top productos (a implementar)."""
    return jsonify([])


@bp.get("/top-customers")
def top_customers():
    """Placeholder: top clientes (a implementar)."""
    return jsonify([])
