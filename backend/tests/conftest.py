"""
Fixtures para tests del backend. Mock de run_query para no depender de Neon en CI.
"""
from datetime import date
from unittest.mock import patch

import pytest

from backend.app import create_app


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def mock_run_query():
    """Simula run_query para tests de endpoints y service sin DB."""

    def fake_run_query(query, params=None):
        if "total_orders" in query and "GROUP BY" not in query:
            return [
                {
                    "total_orders": 100,
                    "total_revenue": 150000.0,
                    "total_sales": 500,
                    "average_order_value": 1500.0,
                }
            ]
        if "date_key" in query and "GROUP BY" in query:
            return [
                {
                    "date_key": date(2024, 1, 1),
                    "total_revenue": 1000.0,
                    "total_orders": 10,
                    "total_quantity": 50,
                },
                {
                    "date_key": date(2024, 1, 2),
                    "total_revenue": 1200.0,
                    "total_orders": 12,
                    "total_quantity": 60,
                },
            ]
        if "dim_products" in query:
            return [
                {
                    "product_id": "P1",
                    "product_name": "Product A",
                    "category": "Cat1",
                    "total_revenue": 5000.0,
                    "total_quantity": 20,
                    "order_count": 5,
                },
            ]
        if "dim_customers" in query:
            return [
                {
                    "customer_id": "C1",
                    "customer_name": "Customer A",
                    "country": "Spain",
                    "city": "Madrid",
                    "total_revenue": 3000.0,
                    "order_count": 3,
                    "total_quantity": 15,
                },
            ]
        return []

    return fake_run_query
