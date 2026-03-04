"""Tests del endpoint GET /analytics/kpis."""
import pytest


def test_kpis_endpoint_returns_200(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/kpis")
    assert response.status_code == 200


def test_kpis_endpoint_has_required_keys(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/kpis")
    data = response.get_json()
    assert "total_revenue" in data
    assert "total_orders" in data
    assert "total_sales" in data
    assert "average_order_value" in data


def test_kpis_endpoint_json_types(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/kpis")
    data = response.get_json()
    assert isinstance(data["total_orders"], int)
    assert isinstance(data["total_revenue"], (int, float))
    assert isinstance(data["total_sales"], int)
    assert isinstance(data["average_order_value"], (int, float))
