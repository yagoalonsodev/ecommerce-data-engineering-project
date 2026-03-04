"""Tests de códigos de respuesta HTTP: 200, 400, 404, 500 (error handling)."""
import pytest


# ——— 200 OK (ya cubiertos en otros archivos; un par de smoke) ———

def test_kpis_200(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/kpis")
    assert response.status_code == 200


def test_root_200(client):
    assert client.get("/").status_code == 200


# ——— 400 Bad Request (parámetros inválidos) ———

def test_top_products_400_invalid_limit_not_integer(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-products?limit=abc")
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data
    assert "Invalid limit" in data.get("error", "")


def test_top_products_400_limit_zero(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-products?limit=0")
    assert response.status_code == 400


def test_top_products_400_limit_negative(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-products?limit=-5")
    assert response.status_code == 400


def test_top_products_400_limit_over_max(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-products?limit=1000")
    assert response.status_code == 400


def test_top_customers_400_invalid_limit(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-customers?limit=not_a_number")
    assert response.status_code == 400


def test_top_customers_400_limit_zero(client, mock_run_query, monkeypatch):
    monkeypatch.setattr("backend.services.analytics_service.run_query", mock_run_query)
    response = client.get("/analytics/top-customers?limit=0")
    assert response.status_code == 400


# ——— 404 Not Found ———

def test_404_nonexistent_route(client):
    response = client.get("/analytics/not-a-real-endpoint")
    assert response.status_code == 404


def test_405_method_not_allowed(client):
    response = client.post("/analytics/kpis")
    assert response.status_code == 405


def test_404_root_typo(client):
    response = client.get("/analytics/kpiss")
    assert response.status_code == 404


# ——— 500 Internal Server Error (service/DB falla) ———

def test_kpis_500_when_service_raises(client, monkeypatch):
    def raise_error(*args, **kwargs):
        raise Exception("DB down")

    monkeypatch.setattr("backend.services.analytics_service.get_kpis", raise_error)
    response = client.get("/analytics/kpis")
    assert response.status_code == 500


def test_sales_by_day_500_when_service_raises(client, monkeypatch):
    def raise_error(*args, **kwargs):
        raise Exception("Connection failed")

    monkeypatch.setattr("backend.services.analytics_service.get_sales_by_day", raise_error)
    response = client.get("/analytics/sales-by-day")
    assert response.status_code == 500


def test_top_products_500_when_service_raises(client, monkeypatch):
    def raise_error(*args, **kwargs):
        raise RuntimeError("DB error")

    monkeypatch.setattr("backend.services.analytics_service.get_top_products", raise_error)
    response = client.get("/analytics/top-products")
    assert response.status_code == 500


def test_top_customers_500_when_service_raises(client, monkeypatch):
    def raise_error(*args, **kwargs):
        raise Exception("Service unavailable")

    monkeypatch.setattr("backend.services.analytics_service.get_top_customers", raise_error)
    response = client.get("/analytics/top-customers")
    assert response.status_code == 500
