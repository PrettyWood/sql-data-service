from typing import Any

import pytest
from fastapi.testclient import TestClient

from sql_data_service.app import PreviewQuery, app
from sql_data_service.dialects import SQLDialect
from sql_data_service.models import SQLQueryDefinition

client = TestClient(app)

ALL_TEST_TABLES = ["labels", "logins", "users"]


@pytest.mark.usefixtures(
    "is_mysql_ready",
    "is_postgresql_ready",
)
@pytest.mark.parametrize(
    "sql_dialect",
    (
        # SQLDialect.MYSQL,
        SQLDialect.POSTGRESQL,
    ),
)
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
        # ~~~~~~~~~~~~~~~ SELECT ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "select", "columns": ["username", "age"]},
            ],
            [
                {"username": "Eric", "age": 30},
                {"username": "Chiara", "age": 31},
                {"username": "Pikachu", "age": 7},
                {"username": "Bulbi", "age": 7},
            ],
        ),
        # ~~~~~~~~~~~~~~~ DELETE ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "delete", "columns": ["username", "age"]},
            ],
            [
                {"city": "Paris"},
                {"city": "Firenze"},
                {"city": "Bourg Palette"},
                {"city": "Bourg Palette"},
            ],
        ),
        # ~~~~~~~~~~~~~~~ RENAME ~~~~~~~~~~~~~~
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
        # ~~~~~~~~~~~~~~~ SORT ~~~~~~~~~~~~~~
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
        # ~~~~~~~~~~~~~~~ FILTER ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "city", "operator": "eq", "value": "Bourg Palette"},
                },
            ],
            [
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "city", "operator": "ne", "value": "Bourg Palette"},
                },
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Chiara", "age": 31, "city": "Firenze"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "age", "operator": "gt", "value": 30},
                },
            ],
            [
                {"username": "Chiara", "age": 31, "city": "Firenze"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "age", "operator": "ge", "value": 30},
                },
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Chiara", "age": 31, "city": "Firenze"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "age", "operator": "lt", "value": 30},
                },
            ],
            [
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "age", "operator": "le", "value": 30},
                },
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "age", "operator": "in", "value": [7, 31]},
                },
            ],
            [
                {"username": "Chiara", "age": 31, "city": "Firenze"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "age", "operator": "nin", "value": [7, 31]},
                },
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {"column": "username", "operator": "matches", "value": "(Er|Pik)"},
                },
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "filter",
                    "condition": {
                        "column": "username",
                        "operator": "notmatches",
                        "value": "(Er|Pik)",
                    },
                },
            ],
            [
                {"username": "Chiara", "age": 31, "city": "Firenze"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "logins"},
                {
                    "name": "filter",
                    "condition": {
                        "column": "type",
                        "operator": "isnull",
                        "value": "",
                    },
                },
            ],
            [
                {"username": "Eric", "login": "2021-05-09", "type": None},
                {"username": "Chiara", "login": "2021-05-08", "type": None},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "logins"},
                {
                    "name": "filter",
                    "condition": {
                        "column": "type",
                        "operator": "notnull",
                        "value": "",
                    },
                },
            ],
            [
                {"username": "Pikachu", "login": "2020-01-01", "type": "Electric"},
                {"username": "Bulbi", "login": "2019-01-01", "type": "Grass/Poison"},
            ],
        ),
        (
            [
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
            [
                {"username": "Eric", "login": "2021-05-09", "type": None},
                {"username": "Chiara", "login": "2021-05-08", "type": None},
                {"username": "Pikachu", "login": "2020-01-01", "type": "Electric"},
            ],
        ),
        # ~~~~~~~~~~~~~~~ UPPERCASE ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "uppercase", "column": "city"},
            ],
            [
                {"username": "Eric", "age": 30, "city": "PARIS"},
                {"username": "Chiara", "age": 31, "city": "FIRENZE"},
                {"username": "Pikachu", "age": 7, "city": "BOURG PALETTE"},
                {"username": "Bulbi", "age": 7, "city": "BOURG PALETTE"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "rename", "to_rename": ["username", "first name"]},
                {"name": "uppercase", "column": "first name"},
            ],
            [
                {"first name": "ERIC", "age": 30, "city": "Paris"},
                {"first name": "CHIARA", "age": 31, "city": "Firenze"},
                {"first name": "PIKACHU", "age": 7, "city": "Bourg Palette"},
                {"first name": "BULBI", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        # ~~~~~~~~~~~~~~~ LOWERCASE ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "lowercase", "column": "city"},
            ],
            [
                {"username": "Eric", "age": 30, "city": "paris"},
                {"username": "Chiara", "age": 31, "city": "firenze"},
                {"username": "Pikachu", "age": 7, "city": "bourg palette"},
                {"username": "Bulbi", "age": 7, "city": "bourg palette"},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "rename", "to_rename": ["username", "first name"]},
                {"name": "lowercase", "column": "first name"},
            ],
            [
                {"first name": "eric", "age": 30, "city": "Paris"},
                {"first name": "chiara", "age": 31, "city": "Firenze"},
                {"first name": "pikachu", "age": 7, "city": "Bourg Palette"},
                {"first name": "bulbi", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        # ~~~~~~~~~~~~~~~ TOP ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {"name": "top", "rank_on": "Value", "sort": "asc", "limit": 3},
            ],
            [
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 7},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "top",
                    "rank_on": "Value",
                    "groups": ["Cartel"],
                    "sort": "desc",
                    "limit": 1,
                },
            ],
            [
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 10},
            ],
        ),
        # ~~~~~~~~~~~~~~~ ARGMAX ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {"name": "argmax", "column": "Value", "groups": ["Cartel"]},
            ],
            [
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 10},
            ],
        ),
        # ~~~~~~~~~~~~~~~ ARGMIN ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {"name": "argmin", "column": "Value", "groups": ["Cartel"]},
            ],
            [
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 7},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1},
            ],
        ),
        # ~~~~~~~~~~~ UNIQUE GROUPS~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels2"},
                {"name": "uniquegroups", "on": ["Label", "Cartel"]},
            ],
            [
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13},
                {"Label": "Label 1", "Cartel": "Cartel 2", "Value": 1},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 7},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20},
            ],
        ),
        # ~~~~~~~~~~~ AGGREGATE (count distinct) ~~~~~~~~~~~~~~
        # ~~~~~~~~~~~ AGGREGATE (count) ~~~~~~~~~~~~~~
        # ~~~~~~~~~~~ AGGREGATE (first) ~~~~~~~~~~~~~~
        # ~~~~~~~~~~~ AGGREGATE (last) ~~~~~~~~~~~~~~
        # ~~~~~~~~~~~ AGGREGATE (sum) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["sumValue"], "agg_function": "sum", "columns": ["Value"]}
                    ],
                },
            ],
            [
                {"Cartel": "Cartel 1", "sumValue": 40},
                {"Cartel": "Cartel 2", "sumValue": 16},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["sumValue"], "agg_function": "sum", "columns": ["Value"]}
                    ],
                    "keep_original_granularity": True,
                },
            ],
            [
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13, "sumValue": 40},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 7, "sumValue": 40},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20, "sumValue": 40},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1, "sumValue": 16},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 10, "sumValue": 16},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5, "sumValue": 16},
            ],
        ),
        # ~~~~~~~~~~~ COMPLEX (agg sum + top) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["sumValue"], "agg_function": "sum", "columns": ["Value"]}
                    ],
                },
                {"name": "top", "rank_on": "sumValue", "sort": "asc", "limit": 2},
            ],
            [
                {"Cartel": "Cartel 2", "sumValue": 16},
                {"Cartel": "Cartel 1", "sumValue": 40},
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
        tables=ALL_TEST_TABLES,
    )
    try:
        response = client.post("/preview", json=preview_query.dict())
    except NotImplementedError as e:
        pytest.skip(reason=str(e))
    else:
        assert response.status_code == 200
        assert response.json() == expected_res
