"""Tests del endpoint GET /health."""


def test_health_returns_200(client):
    response = client.get("/health")
    assert response.status_code == 200


def test_health_returns_ok_status(client):
    response = client.get("/health")
    data = response.get_json()
    assert data == {"status": "ok"}


def test_health_returns_json(client):
    response = client.get("/health")
    assert "application/json" in (response.content_type or "")


def test_health_post_returns_405(client):
    response = client.post("/health")
    assert response.status_code == 405


def test_health_put_returns_405(client):
    response = client.put("/health")
    assert response.status_code == 405
