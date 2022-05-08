from typing import Any

import pytest
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect
from sql_data_service.translate import translate_pipeline

ALL_TABLES_COLUMNS = {"users": ["username", "age", "city"]}

PIPELINES = [
    [
        {"name": "domain", "domain": "users"},
    ],
    [
        {"name": "domain", "domain": "users"},
        {"name": "select", "columns": ["username", "age"]},
    ],
]


@pytest.mark.parametrize(
    "sql_dialect,pipeline_steps,expected_query",
    [
        (
            SQLDialect.MYSQL,
            PIPELINES[0],
            "SELECT * FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES[0],
            'SELECT * FROM "users"',
        ),
        (
            SQLDialect.MYSQL,
            PIPELINES[1],
            "SELECT `username`,`age` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES[1],
            'SELECT "username","age" FROM "users"',
        ),
    ],
)
def test_translate_pipeline(
    sql_dialect: SQLDialect,
    pipeline_steps: list[dict[str, Any]],
    expected_query: str,
) -> None:
    query = translate_pipeline(
        sql_dialect=sql_dialect,
        pipeline=PipelineWithVariables(steps=pipeline_steps),
        tables_columns=ALL_TABLES_COLUMNS,
    )
    assert query == expected_query
