import pytest
from fastapi.testclient import TestClient

from sql_data_service.app import PreviewQuery, app
from sql_data_service.dialects import SQLDialect
from sql_data_service.models import SQLQueryDefinition

client = TestClient(app)


@pytest.mark.usefixtures(
    "is_mysql_ready",
    "is_postgresql_ready",
)
@pytest.mark.parametrize("sql_dialect", SQLDialect.__members__.values())
def test_get_preview_mysql(
    sql_dialect: SQLDialect,
    request: pytest.FixtureRequest,
) -> None:
    sql_connection_config = request.getfixturevalue(f"{sql_dialect}_connection_config")
    sql_query_definition = SQLQueryDefinition(
        connection={
            "dialect": sql_dialect.value,
            "config": sql_connection_config,
        },
        pipeline={"steps": [{"name": "domain", "domain": "users"}]},
    )
    preview_query = PreviewQuery(
        query_def=sql_query_definition,
        tables=["users"],
    )
    response = client.post("/preview", json=preview_query.dict())

    assert response.status_code == 200
    assert response.json() == [
        {"username": "Eric", "age": 30, "city": "Paris"},
        {"username": "Chiara", "age": 31, "city": "Firenze"},
        {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
    ]
