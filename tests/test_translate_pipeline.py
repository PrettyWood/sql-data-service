from typing import Any

import pytest
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect
from sql_data_service.translate import translate_pipeline

ALL_TABLES_COLUMNS = [
    {"users": ["username", "age", "city"]},
]

PIPELINES = [
    [{"name": "domain", "domain": "users"}],
]


@pytest.mark.parametrize(
    "sql_dialect,pipeline_steps,tables_columns,expected_query",
    [
        (
            SQLDialect.MYSQL,
            PIPELINES[0],
            ALL_TABLES_COLUMNS[0],
            "SELECT `username`,`age`,`city` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES[0],
            ALL_TABLES_COLUMNS[0],
            'SELECT "username","age","city" FROM "users"',
        ),
    ],
)
def test_translate_pipeline(
    sql_dialect: SQLDialect,
    pipeline_steps: list[dict[str, Any]],
    tables_columns: dict[str, list[str]],
    expected_query: str,
) -> None:
    query = translate_pipeline(
        sql_dialect=sql_dialect,
        pipeline=PipelineWithVariables(steps=pipeline_steps),
        tables_columns=tables_columns,
    )
    assert query == expected_query
