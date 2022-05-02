import pytest
from fastapi.testclient import TestClient

from sql_data_service.main import app
from sql_data_service.models import MySQLConnectionConfig, PostgreSQLConnectionConfig

client = TestClient(app)


@pytest.mark.usefixtures("is_mysql_ready")
def test_get_preview_mysql(mysql_connection_config: MySQLConnectionConfig) -> None:
    response = client.post(
        "/preview",
        json={
            "connection": {
                "dialect": "mysql",
                "config": mysql_connection_config.dict(),
            },
            "pipeline": {
                "steps": [{"name": "domain", "domain": "users"}],
            },
        },
    )
    assert response.status_code == 200
    assert response.json() == [
        {"username": "Eric", "age": 30, "city": "Paris"},
        {"username": "Chiara", "age": 31, "city": "Firenze"},
        {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
    ]


@pytest.mark.usefixtures("is_postgresql_ready")
def test_get_preview_postgresql(postgresql_connection_config: PostgreSQLConnectionConfig) -> None:
    response = client.post(
        "/preview",
        json={
            "connection": {
                "dialect": "postgresql",
                "config": postgresql_connection_config.dict(),
            },
            "pipeline": {
                "steps": [{"name": "domain", "domain": "users"}],
            },
        },
    )
    assert response.status_code == 200
    assert response.json() == [
        {"username": "Eric", "age": 30, "city": "Paris"},
        {"username": "Chiara", "age": 31, "city": "Firenze"},
        {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
    ]
