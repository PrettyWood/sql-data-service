from fastapi.testclient import TestClient

from sql_data_service.main import app

client = TestClient(app)


def test_get_status():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "OK", "version": "0.0.0"}
