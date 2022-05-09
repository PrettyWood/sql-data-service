from typing import Any

import pytest
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect
from sql_data_service.translate import translate_pipeline

ALL_TABLES_COLUMNS = {"users": ["username", "age", "city"]}

PIPELINES = {
    "domain": [
        {"name": "domain", "domain": "users"},
    ],
    "select": [
        {"name": "domain", "domain": "users"},
        {"name": "select", "columns": ["username", "age"]},
    ],
    "rename": [
        {"name": "domain", "domain": "users"},
        {"name": "select", "columns": ["username", "age"]},
        {"name": "rename", "to_rename": ["username", "first name"]},
    ],
}


@pytest.mark.parametrize(
    "sql_dialect,pipeline_steps,tables_columns,expected_query",
    [
        # pipeline domain
        (
            SQLDialect.MYSQL,
            PIPELINES["domain"],
            None,
            "SELECT * FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["domain"],
            None,
            'SELECT * FROM "users"',
        ),
        # pipeline select (without table names)
        (
            SQLDialect.MYSQL,
            PIPELINES["select"],
            None,
            "SELECT `username`,`age` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["select"],
            None,
            'SELECT "username","age" FROM "users"',
        ),
        # pipeline select (with table names)
        (
            SQLDialect.MYSQL,
            PIPELINES["select"],
            ALL_TABLES_COLUMNS,
            "SELECT `username`,`age` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["select"],
            ALL_TABLES_COLUMNS,
            'SELECT "username","age" FROM "users"',
        ),
        # pipeline rename
        (
            SQLDialect.MYSQL,
            PIPELINES["rename"],
            ALL_TABLES_COLUMNS,
            "SELECT `username` `first name`,`age` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["rename"],
            ALL_TABLES_COLUMNS,
            'SELECT "username" "first name","age" FROM "users"',
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
