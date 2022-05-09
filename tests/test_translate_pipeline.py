from typing import Any

import pytest
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect
from sql_data_service.translate import translate_pipeline

ALL_TABLES_COLUMNS = {
    "logins": ["username", "login", "type"],
    "users": ["username", "age", "city"],
}

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
    "delete": [
        {"name": "domain", "domain": "users"},
        {"name": "select", "columns": ["username", "age", "city"]},
        {"name": "delete", "columns": ["username", "city"]},
    ],
    "sort": [
        {"name": "domain", "domain": "users"},
        {
            "name": "sort",
            "columns": [{"column": "age", "order": "asc"}, {"column": "username", "order": "desc"}],
        },
    ],
    "filter": [
        {"name": "domain", "domain": "users"},
        {
            "name": "filter",
            "condition": {"column": "username", "operator": "eq", "value": "Pikachu"},
        },
    ],
    "filter2": [
        {"name": "domain", "domain": "logins"},
        {
            "name": "filter",
            "condition": {
                "or": [
                    {
                        "column": "username",
                        "operator": "eq",
                        "value": "Eric",
                    },
                    {
                        "column": "username",
                        "operator": "matches",
                        "value": "Chia",
                    },
                    {
                        "and": [
                            {
                                "column": "type",
                                "operator": "notnull",
                                "value": "",
                            },
                            {
                                "column": "login",
                                "operator": "ge",
                                "value": "2020-01-01",
                            },
                        ]
                    },
                ]
            },
        },
    ],
}


@pytest.mark.parametrize(
    "sql_dialect,pipeline_steps,tables_columns,expected_query",
    [
        # pipeline domain (without table names)
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
        # pipeline domain (with table names)
        (
            SQLDialect.MYSQL,
            PIPELINES["domain"],
            ALL_TABLES_COLUMNS,
            "SELECT `username`,`age`,`city` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["domain"],
            ALL_TABLES_COLUMNS,
            'SELECT "username","age","city" FROM "users"',
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
        # pipeline delete
        (
            SQLDialect.MYSQL,
            PIPELINES["delete"],
            ALL_TABLES_COLUMNS,
            "SELECT `age` FROM `users`",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["delete"],
            ALL_TABLES_COLUMNS,
            'SELECT "age" FROM "users"',
        ),
        # pipeline sort
        (
            SQLDialect.MYSQL,
            PIPELINES["sort"],
            ALL_TABLES_COLUMNS,
            "SELECT `username`,`age`,`city` FROM `users` ORDER BY `age` ASC,`username` DESC",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["sort"],
            ALL_TABLES_COLUMNS,
            'SELECT "username","age","city" FROM "users" ORDER BY "age" ASC,"username" DESC',
        ),
        # pipeline filter
        (
            SQLDialect.MYSQL,
            PIPELINES["filter"],
            ALL_TABLES_COLUMNS,
            "SELECT `username`,`age`,`city` FROM `users` WHERE `username`='Pikachu'",
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["filter"],
            ALL_TABLES_COLUMNS,
            'SELECT "username","age","city" FROM "users" WHERE "username"=\'Pikachu\'',
        ),
        # pipeline filter
        (
            SQLDialect.MYSQL,
            PIPELINES["filter2"],
            ALL_TABLES_COLUMNS,
            (
                "SELECT `username`,`login`,`type` FROM `logins` "
                "WHERE `username`='Eric' OR `username` REGEXP 'Chia' OR (`type` IS NOT NULL AND `login`>='2020-01-01')"
            ),
        ),
        (
            SQLDialect.POSTGRESQL,
            PIPELINES["filter2"],
            ALL_TABLES_COLUMNS,
            (
                'SELECT "username","login","type" FROM "logins" '
                'WHERE "username"=\'Eric\' OR "username" SIMILAR TO \'%Chia%\' OR ("type" IS NOT NULL AND "login">=\'2020-01-01\')'
            ),
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
