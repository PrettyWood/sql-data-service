from fastapi.testclient import TestClient

from sql_data_service.app import app

client = TestClient(app)


def test_get_status() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "OK", "version": "0.0.0"}
