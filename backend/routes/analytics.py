"""
Rutas de analytics. Route → Service → DB (arquitectura limpia).
"""

from flask import Blueprint, jsonify, request

try:
    import backend.services.analytics_service as analytics_service
except ImportError:
    import services.analytics_service as analytics_service

bp = Blueprint("analytics", __name__, url_prefix="/analytics")


@bp.get("/kpis")
def get_kpis():
    """GET /analytics/kpis — total_sales, total_revenue, total_orders, average_order_value."""
    return jsonify(analytics_service.get_kpis())  # Route → Service → DB


@bp.get("/sales-by-day")
def sales_by_day():
    """GET /analytics/sales-by-day — ventas agregadas por día."""
    return jsonify(analytics_service.get_sales_by_day())


def _get_limit_param(max_limit=100):
    """Valida query param limit (1..max_limit). Devuelve (limit, error_response). error_response no None → 400."""
    raw = request.args.get("limit")
    if raw is None:
        return 10, None
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return None, ({"error": "Invalid limit", "detail": "limit must be an integer"}, 400)
    if limit < 1 or limit > max_limit:
        return None, ({"error": "Invalid limit", "detail": f"limit must be between 1 and {max_limit}"}, 400)
    return limit, None


@bp.get("/top-products")
def top_products():
    """GET /analytics/top-products — top productos por revenue (query param: limit, default 10)."""
    limit, err = _get_limit_param()
    if err is not None:
        return jsonify(err[0]), err[1]
    return jsonify(analytics_service.get_top_products(limit=limit))


@bp.get("/top-customers")
def top_customers():
    """GET /analytics/top-customers — top clientes por revenue (query param: limit, default 10)."""
    limit, err = _get_limit_param()
    if err is not None:
        return jsonify(err[0]), err[1]
    return jsonify(analytics_service.get_top_customers(limit=limit))
