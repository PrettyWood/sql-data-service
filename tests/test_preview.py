from fastapi.testclient import TestClient

from sql_data_service.main import app

client = TestClient(app)


def test_get_preview():
    response = client.post(
        "/preview",
        json={
            "connection": {
                "host": "127.0.0.1",
                "port": 5432,
                "user": "postgres",
                "password": "pikapika",
            },
            "pipeline": {
                "steps": [{"name": "domain", "domain": "table_test"}],
            },
        },
    )
    assert response.status_code == 200
    assert response.json() == [
        {"username": "Eric", "age": 30, "city": "Paris"},
        {"username": "Chiara", "age": 31, "city": "Florence"},
        {"age": 34, "city": "Paris", "username": "Laure"},
    ]
