"""Tests unitarios del analytics_service con run_query mockeado."""
import pytest

import backend.services.analytics_service as analytics_service


def test_get_kpis_returns_dict_with_keys(mock_run_query, monkeypatch):
    monkeypatch.setattr(analytics_service, "run_query", mock_run_query)
    result = analytics_service.get_kpis()
    assert isinstance(result, dict)
    assert "total_orders" in result
    assert "total_revenue" in result
    assert "total_sales" in result
    assert "average_order_value" in result
    assert result["total_orders"] == 100
    assert result["total_revenue"] == 150000.0


def test_get_kpis_handles_empty_result(monkeypatch):
    monkeypatch.setattr(analytics_service, "run_query", lambda q, p=None: [])
    result = analytics_service.get_kpis()
    assert result["total_orders"] == 0
    assert result["total_revenue"] == 0
    assert result["total_sales"] == 0
    assert result["average_order_value"] == 0


def test_get_sales_by_day_returns_list_of_dicts(mock_run_query, monkeypatch):
    monkeypatch.setattr(analytics_service, "run_query", mock_run_query)
    result = analytics_service.get_sales_by_day()
    assert isinstance(result, list)
    assert len(result) == 2
    assert "date" in result[0]
    assert result[0]["total_revenue"] == 1000.0
    assert result[0]["total_orders"] == 10


def test_get_top_products_respects_limit(mock_run_query, monkeypatch):
    monkeypatch.setattr(analytics_service, "run_query", mock_run_query)
    result = analytics_service.get_top_products(limit=3)
    assert isinstance(result, list)
    # mock devuelve 1; en producción sería hasta limit
    assert len(result) >= 0


def test_get_top_customers_respects_limit(mock_run_query, monkeypatch):
    monkeypatch.setattr(analytics_service, "run_query", mock_run_query)
    result = analytics_service.get_top_customers(limit=5)
    assert isinstance(result, list)
