"""
Lógica de negocio para analytics. Llama a db.run_query(); no contiene SQL en las routes.
Endpoints: KPIs, sales-by-day, top-products, top-customers (a implementar).
"""

from backend.db import run_query


def get_kpis() -> dict:
    """
    KPIs agregados: total_sales, total_revenue, total_orders, average_order_value.
    Devuelve un único dict para GET /analytics/kpis.
    """
    # TODO: consulta a fact_sales cuando se implementen los endpoints
    row = run_query(
        """
        SELECT
            COUNT(*) AS total_orders,
            COALESCE(SUM(total_amount), 0) AS total_revenue,
            COALESCE(SUM(quantity), 0) AS total_sales,
            COALESCE(AVG(total_amount), 0) AS average_order_value
        FROM fact_sales
        """
    )
    if not row:
        return {
            "total_orders": 0,
            "total_revenue": 0,
            "total_sales": 0,
            "average_order_value": 0,
        }
    r = row[0]
    return {
        "total_orders": int(r.get("total_orders", 0)),
        "total_revenue": float(r.get("total_revenue", 0)),
        "total_sales": int(r.get("total_sales", 0)),
        "average_order_value": float(r.get("average_order_value", 0)),
    }
