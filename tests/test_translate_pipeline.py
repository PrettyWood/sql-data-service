from typing import Any

import pytest
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.translate import SQLDialect, translate_pipeline

ALL_TABLES_COLUMNS = {
    "users": ["username", "age", "city"],
}

PIPELINES = [
    [{"name": "domain", "domain": "users"}],
]


@pytest.mark.parametrize(
    "sql_dialect,pipeline_steps,all_columns,expected_query",
    [
        (
            "mysql",
            PIPELINES[0],
            ALL_TABLES_COLUMNS["users"],
            'SELECT "username","age","city" FROM "users"',
        ),
        (
            "postgresql",
            PIPELINES[0],
            ALL_TABLES_COLUMNS["users"],
            'SELECT "username","age","city" FROM "users"',
        ),
    ],
)
async def test_translate_pipeline(
    sql_dialect: SQLDialect,
    pipeline_steps: list[dict[str, Any]],
    all_columns: list[str],
    expected_query: str,
) -> None:
    pipeline = PipelineWithVariables(steps=pipeline_steps)
    query = await translate_pipeline(sql_dialect, pipeline, all_columns)
    assert query == expected_query
