"""Tests de endpoints /analytics/sales-by-day, top-products, top-customers."""
import pytest


def test_sales_by_day_returns_200(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/sales-by-day")
    assert response.status_code == 200


def test_sales_by_day_returns_list(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/sales-by-day")
    data = response.get_json()
    assert isinstance(data, list)
    if data:
        assert "date" in data[0]
        assert "total_revenue" in data[0]
        assert "total_orders" in data[0]
        assert "total_quantity" in data[0]


def test_top_products_returns_200(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-products")
    assert response.status_code == 200


def test_top_products_returns_list(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-products?limit=5")
    data = response.get_json()
    assert isinstance(data, list)
    if data:
        assert "product_id" in data[0] or "product_name" in data[0]
        assert "total_revenue" in data[0]


def test_top_customers_returns_200(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-customers")
    assert response.status_code == 200


def test_top_customers_returns_list(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-customers?limit=5")
    data = response.get_json()
    assert isinstance(data, list)
    if data:
        assert "customer_id" in data[0] or "customer_name" in data[0]
        assert "total_revenue" in data[0]


def test_root_returns_service_info(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.get_json()
    assert "service" in data
    assert "ecommerce" in data["service"].lower() or "api" in data["service"].lower()
