"""
Rutas de analytics. Route → Service → DB (arquitectura limpia).
"""

from flask import Blueprint, jsonify, request

import backend.services.analytics_service as analytics_service

bp = Blueprint("analytics", __name__, url_prefix="/analytics")


@bp.get("/kpis")
def get_kpis():
    """GET /analytics/kpis — total_sales, total_revenue, total_orders, average_order_value."""
    return jsonify(analytics_service.get_kpis())  # Route → Service → DB


@bp.get("/sales-by-day")
def sales_by_day():
    """GET /analytics/sales-by-day — ventas agregadas por día."""
    return jsonify(analytics_service.get_sales_by_day())


@bp.get("/top-products")
def top_products():
    """GET /analytics/top-products — top productos por revenue (query param: limit, default 10)."""
    limit = request.args.get("limit", 10, type=int)
    limit = min(max(1, limit), 100)
    return jsonify(analytics_service.get_top_products(limit=limit))


@bp.get("/top-customers")
def top_customers():
    """GET /analytics/top-customers — top clientes por revenue (query param: limit, default 10)."""
    limit = request.args.get("limit", 10, type=int)
    limit = min(max(1, limit), 100)
    return jsonify(analytics_service.get_top_customers(limit=limit))
