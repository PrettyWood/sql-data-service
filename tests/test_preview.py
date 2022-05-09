from typing import Any

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
@pytest.mark.parametrize(
    "pipeline_steps,expected_res",
    (
        (
            [{"name": "domain", "domain": "users"}],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Chiara", "age": 31, "city": "Firenze"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "select", "columns": ["username", "age"]},
                {"name": "rename", "to_rename": ["username", "first name"]},
            ],
            [
                {"first name": "Eric", "age": 30},
                {"first name": "Chiara", "age": 31},
                {"first name": "Pikachu", "age": 7},
                {"first name": "Bulbi", "age": 7},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "sort",
                    "columns": [
                        {"column": "age", "order": "desc"},
                        {"column": "username", "order": "asc"},
                    ],
                },
            ],
            [
                {"username": "Chiara", "age": 31, "city": "Firenze"},
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
            ],
        ),
    ),
)
def test_get_preview_mysql(
    sql_dialect: SQLDialect,
    pipeline_steps: list[dict[str, str]],
    expected_res: list[dict[str, Any]],
    request: pytest.FixtureRequest,
) -> None:
    sql_connection_config = request.getfixturevalue(f"{sql_dialect}_connection_config")
    sql_query_definition = SQLQueryDefinition(
        connection={
            "dialect": sql_dialect.value,
            "config": sql_connection_config,
        },
        pipeline={"steps": pipeline_steps},
    )
    preview_query = PreviewQuery(
        query_def=sql_query_definition,
        tables=["users"],
    )
    response = client.post("/preview", json=preview_query.dict())

    assert response.status_code == 200
    assert response.json() == expected_res
