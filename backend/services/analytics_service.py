"""
Lógica de negocio para analytics. Llama a db.run_query(); no contiene SQL en las routes.
Route → Service → DB. Config vía Settings.get_database_url() en backend.db.
"""

try:
    from backend.db import run_query
except ImportError:
    from db import run_query


def get_kpis() -> dict:
    """
    KPIs agregados: total_sales, total_revenue, total_orders, average_order_value.
    Devuelve un único dict para GET /analytics/kpis.
    """
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


def get_sales_by_day() -> list[dict]:
    """
    Ventas agregadas por día: date_key, total_revenue, total_orders, total_quantity.
    Para GET /analytics/sales-by-day.
    """
    rows = run_query(
        """
        SELECT
            date_key,
            SUM(total_revenue) AS total_revenue,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(quantity) AS total_quantity
        FROM fact_sales
        GROUP BY date_key
        ORDER BY date_key
        """
    )
    return [
        {
            "date": r["date_key"].isoformat() if r.get("date_key") else None,
            "total_revenue": float(r.get("total_revenue", 0)),
            "total_orders": int(r.get("total_orders", 0)),
            "total_quantity": int(r.get("total_quantity", 0)),
        }
        for r in rows
    ]


def get_top_products(limit: int = 10) -> list[dict]:
    """
    Top productos por revenue (join fact_sales + dim_products).
    Para GET /analytics/top-products.
    """
    rows = run_query(
        """
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            SUM(f.total_revenue) AS total_revenue,
            SUM(f.quantity) AS total_quantity,
            COUNT(DISTINCT f.order_id) AS order_count
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_revenue DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [
        {
            "product_id": r.get("product_id"),
            "product_name": r.get("product_name"),
            "category": r.get("category"),
            "total_revenue": float(r.get("total_revenue", 0)),
            "total_quantity": int(r.get("total_quantity", 0)),
            "order_count": int(r.get("order_count", 0)),
        }
        for r in rows
    ]


def get_top_customers(limit: int = 10) -> list[dict]:
    """
    Top clientes por revenue (join fact_sales + dim_customers).
    Para GET /analytics/top-customers.
    """
    rows = run_query(
        """
        SELECT
            c.customer_id,
            c.customer_name,
            c.country,
            c.city,
            SUM(f.total_revenue) AS total_revenue,
            COUNT(DISTINCT f.order_id) AS order_count,
            SUM(f.quantity) AS total_quantity
        FROM fact_sales f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        GROUP BY c.customer_id, c.customer_name, c.country, c.city
        ORDER BY total_revenue DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [
        {
            "customer_id": r.get("customer_id"),
            "customer_name": r.get("customer_name"),
            "country": r.get("country"),
            "city": r.get("city"),
            "total_revenue": float(r.get("total_revenue", 0)),
            "order_count": int(r.get("order_count", 0)),
            "total_quantity": int(r.get("total_quantity", 0)),
        }
        for r in rows
    ]
