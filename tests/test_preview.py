from typing import Any

import pytest
from fastapi.testclient import TestClient

from sql_data_service.app import PreviewQuery, app
from sql_data_service.dialects import SQLDialect
from sql_data_service.models import SQLQueryDefinition

client = TestClient(app)

ALL_TEST_TABLES = ["labels", "labels2", "logins", "logins2", "users"]


@pytest.mark.usefixtures(
    "is_mysql_ready",
    "is_postgresql_ready",
)
@pytest.mark.parametrize(
    "sql_dialect",
    (
        SQLDialect.MYSQL,
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
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6},
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
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12},
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
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12},
            ],
        ),
        # ~~~~~~~~~~~~~~~ ARGMIN ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {"name": "argmin", "column": "Value", "groups": ["Cartel"]},
            ],
            [
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6},
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
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20},
            ],
        ),
        # ~~~~~~~~~~~ AGGREGATE (avg) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["avgValue"], "agg_function": "avg", "columns": ["Value"]}
                    ],
                },
            ],
            [
                {"Cartel": "Cartel 1", "avgValue": 13.0},
                {"Cartel": "Cartel 2", "avgValue": 6.0},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["avgValue"], "agg_function": "avg", "columns": ["Value"]}
                    ],
                    "keep_original_granularity": True,
                },
            ],
            [
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13, "avgValue": 13.0},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6, "avgValue": 13.0},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20, "avgValue": 13.0},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1, "avgValue": 6.0},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12, "avgValue": 6.0},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5, "avgValue": 6.0},
            ],
        ),
        # ~~~~~~~~~~~ AGGREGATE (count) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "aggregate",
                    "on": ["city"],
                    "aggregations": [
                        {
                            "new_columns": ["countAge"],
                            "agg_function": "count",
                            "columns": ["age"],
                        }
                    ],
                },
            ],
            [
                {"city": "Bourg Palette", "countAge": 2},
                {"city": "Firenze", "countAge": 1},
                {"city": "Paris", "countAge": 1},
            ],
        ),
        # ~~~~~~~~~~~ AGGREGATE (count distinct) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "aggregate",
                    "on": ["city"],
                    "aggregations": [
                        {
                            "new_columns": ["countDistinctAge"],
                            "agg_function": "count distinct",
                            "columns": ["age"],
                        }
                    ],
                },
            ],
            [
                {"city": "Bourg Palette", "countDistinctAge": 1},
                {"city": "Firenze", "countDistinctAge": 1},
                {"city": "Paris", "countDistinctAge": 1},
            ],
        ),
        # # ~~~~~~~~~~~ AGGREGATE (first) ~~~~~~~~~~~~~~
        # (
        #     [
        #         {"name": "domain", "domain": "users"},
        #         {
        #             "name": "aggregate",
        #             "on": ["city"],
        #             "aggregations": [
        #                 {
        #                     "new_columns": ["firstUsername"],
        #                     "agg_function": "first",
        #                     "columns": ["username"],
        #                 }
        #             ],
        #         },
        #     ],
        #     [
        #         {"firstUsername": "Eric", "city": "Paris"},
        #         {"firstUsername": "Chiara", "city": "Firenze"},
        #         {"firstUsername": "Pikachu", "city": "Bourg Palette"},
        #     ],
        # ),
        # (
        #     [
        #         {"name": "domain", "domain": "users"},
        #         {
        #             "name": "aggregate",
        #             "on": ["city"],
        #             "aggregations": [
        #                 {
        #                     "new_columns": ["firstUsername"],
        #                     "agg_function": "first",
        #                     "columns": ["username"],
        #                 }
        #             ],
        #             "keep_original_granularity": True,
        #         },
        #     ],
        #     [
        #         {"username": "Eric", "age": 30, "city": "Paris", "firstUsername": "Eric"},
        #         {"username": "Chiara", "age": 31, "city": "Firenze", "firstUsername": "Chiara"},
        #         {
        #             "username": "Pikachu",
        #             "age": 7,
        #             "city": "Bourg Palette",
        #             "firstUsername": "Pikachu",
        #         },
        #         {
        #             "username": "Bulbi",
        #             "age": 7,
        #             "city": "Bourg Palette",
        #             "firstUsername": "Pikachu",
        #         },
        #     ],
        # ),
        # # ~~~~~~~~~~~ AGGREGATE (last) ~~~~~~~~~~~~~~
        # (
        #     [
        #         {"name": "domain", "domain": "users"},
        #         {
        #             "name": "aggregate",
        #             "on": ["city"],
        #             "aggregations": [
        #                 {"new_columns": ["lastUsername"], "agg_function": "last", "columns": ["username"]}
        #             ],
        #         },
        #     ],
        #     [
        #         {"lastUsername": "Eric", "city": "Paris"},
        #         {"lastUsername": "Chiara", "city": "Firenze"},
        #         {"lastUsername": "Bulbi", "city": "Bourg Palette"},
        #     ],
        # ),
        # (
        #     [
        #         {"name": "domain", "domain": "users"},
        #         {
        #             "name": "aggregate",
        #             "on": ["city"],
        #             "aggregations": [
        #                 {"new_columns": ["lastUsername"], "agg_function": "last", "columns": ["username"]}
        #             ],
        #             "keep_original_granularity": True,
        #         },
        #     ],
        #     [
        #         {"username": "Eric", "age": 30, "city": "Paris", "lastUsername": "Eric"},
        #         {"username": "Chiara", "age": 31, "city": "Firenze", "lastUsername": "Chiara"},
        #         {"username": "Pikachu", "age": 7, "city": "Bourg Palette", "lastUsername": "Bulbi"},
        #         {"username": "Bulbi", "age": 7, "city": "Bourg Palette", "lastUsername": "Bulbi"},
        #     ],
        # ),
        # ~~~~~~~~~~~ AGGREGATE (max) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["maxValue"], "agg_function": "max", "columns": ["Value"]}
                    ],
                },
            ],
            [
                {"Cartel": "Cartel 1", "maxValue": 20},
                {"Cartel": "Cartel 2", "maxValue": 12},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["maxValue"], "agg_function": "max", "columns": ["Value"]}
                    ],
                    "keep_original_granularity": True,
                },
            ],
            [
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13, "maxValue": 20},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6, "maxValue": 20},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20, "maxValue": 20},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1, "maxValue": 12},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12, "maxValue": 12},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5, "maxValue": 12},
            ],
        ),
        # ~~~~~~~~~~~ AGGREGATE (min) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["minValue"], "agg_function": "min", "columns": ["Value"]}
                    ],
                },
            ],
            [
                {"Cartel": "Cartel 1", "minValue": 6},
                {"Cartel": "Cartel 2", "minValue": 1},
            ],
        ),
        (
            [
                {"name": "domain", "domain": "labels"},
                {
                    "name": "aggregate",
                    "on": ["Cartel"],
                    "aggregations": [
                        {"new_columns": ["minValue"], "agg_function": "min", "columns": ["Value"]}
                    ],
                    "keep_original_granularity": True,
                },
            ],
            [
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13, "minValue": 6},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6, "minValue": 6},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20, "minValue": 6},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1, "minValue": 1},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12, "minValue": 1},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5, "minValue": 1},
            ],
        ),
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
                {"Cartel": "Cartel 1", "sumValue": 39},
                {"Cartel": "Cartel 2", "sumValue": 18},
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
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13, "sumValue": 39},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6, "sumValue": 39},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20, "sumValue": 39},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1, "sumValue": 18},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12, "sumValue": 18},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5, "sumValue": 18},
            ],
        ),
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
                {"Cartel": "Cartel 1", "sumValue": 39},
                {"Cartel": "Cartel 2", "sumValue": 18},
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
                {"Label": "Label 1", "Cartel": "Cartel 1", "Value": 13, "sumValue": 39},
                {"Label": "Label 2", "Cartel": "Cartel 1", "Value": 6, "sumValue": 39},
                {"Label": "Label 3", "Cartel": "Cartel 1", "Value": 20, "sumValue": 39},
                {"Label": "Label 4", "Cartel": "Cartel 2", "Value": 1, "sumValue": 18},
                {"Label": "Label 5", "Cartel": "Cartel 2", "Value": 12, "sumValue": 18},
                {"Label": "Label 6", "Cartel": "Cartel 2", "Value": 5, "sumValue": 18},
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
                {"Cartel": "Cartel 2", "sumValue": 18},
                {"Cartel": "Cartel 1", "sumValue": 39},
            ],
        ),
        # ~~~~~~~~~~~ TEXT ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "text", "text": "Stew", "new_column": "cat"},
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris", "cat": "Stew"},
                {"username": "Chiara", "age": 31, "city": "Firenze", "cat": "Stew"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette", "cat": "Stew"},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette", "cat": "Stew"},
            ],
        ),
        # ~~~~~~~~~~~ FORMULA (without spaces and strings) ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "formula", "formula": "(age + 1) * 2", "new_column": "result"},
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris", "result": 62},
                {"username": "Chiara", "age": 31, "city": "Firenze", "result": 64},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette", "result": 16},
                {"username": "Bulbi", "age": 7, "city": "Bourg Palette", "result": 16},
            ],
        ),
        # ~~~~~~~~~~~ CONVERT ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {"name": "convert", "columns": ["age"], "data_type": "text"},
            ],
            [
                {"username": "Eric", "age": "30", "city": "Paris"},
                {"username": "Chiara", "age": "31", "city": "Firenze"},
                {"username": "Pikachu", "age": "7", "city": "Bourg Palette"},
                {"username": "Bulbi", "age": "7", "city": "Bourg Palette"},
            ],
        ),
        # ~~~~~~~~~~~ REPLACE ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "replace",
                    "search_column": "username",
                    "to_replace": [["Eric", "Michel"], ["Bulbi", "Cara"]],
                },
            ],
            [
                {"username": "Michel", "age": 30, "city": "Paris"},
                {"username": "Chiara", "age": 31, "city": "Firenze"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "Cara", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        # ~~~~~~~~~~~ REPLACE + TRIM ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "replace",
                    "search_column": "username",
                    "to_replace": [["Bulbi", "  spaces  "]],
                },
                {"name": "trim", "columns": ["username"]},
            ],
            [
                {"username": "Eric", "age": 30, "city": "Paris"},
                {"username": "Chiara", "age": 31, "city": "Firenze"},
                {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
                {"username": "spaces", "age": 7, "city": "Bourg Palette"},
            ],
        ),
        # ~~~~~~~~~~~ REPLACE + split ~~~~~~~~~~~~~~
        (
            [
                {"name": "domain", "domain": "users"},
                {
                    "name": "replace",
                    "search_column": "city",
                    "to_replace": [["Paris", "Vaurg Paris"], ["Firenze", "Naurg Firenze"]],
                },
                {"name": "split", "column": "city", "delimiter": "rg ", "number_cols_to_keep": 2},
            ],
            [
                {"username": "Eric", "age": 30, "city_1": "Vau", "city_2": "Paris"},
                {"username": "Chiara", "age": 31, "city_1": "Nau", "city_2": "Firenze"},
                {"username": "Pikachu", "age": 7, "city_1": "Bou", "city_2": "Palette"},
                {"username": "Bulbi", "age": 7, "city_1": "Bou", "city_2": "Palette"},
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


@pytest.mark.usefixtures(
    "is_mysql_ready",
    "is_postgresql_ready",
)
@pytest.mark.parametrize(
    "sql_dialect",
    (
        SQLDialect.MYSQL,
        SQLDialect.POSTGRESQL,
    ),
)
def test_fromdate(sql_dialect: SQLDialect, request: pytest.FixtureRequest) -> None:
    sql_connection_config = request.getfixturevalue(f"{sql_dialect}_connection_config")

    if sql_dialect == SQLDialect.MYSQL:
        date_format = "%d/%m/%Y"
    else:
        date_format = "DD/MM/YYYY"

    sql_query_definition = SQLQueryDefinition(
        connection={
            "dialect": sql_dialect.value,
            "config": sql_connection_config,
        },
        pipeline={
            "steps": [
                {"name": "domain", "domain": "logins"},
                {"name": "fromdate", "column": "login", "format": date_format},
            ],
        },
    )
    preview_query = PreviewQuery(
        query_def=sql_query_definition,
        tables=ALL_TEST_TABLES,
    )
    response = client.post("/preview", json=preview_query.dict())
    assert response.status_code == 200
    assert response.json() == [
        {"username": "Eric", "login": "09/05/2021", "type": None},
        {"username": "Chiara", "login": "08/05/2021", "type": None},
        {"username": "Pikachu", "login": "01/01/2020", "type": "Electric"},
        {"username": "Bulbi", "login": "01/01/2019", "type": "Grass/Poison"},
    ]


@pytest.mark.usefixtures(
    "is_mysql_ready",
    "is_postgresql_ready",
)
@pytest.mark.parametrize(
    "sql_dialect",
    (
        SQLDialect.MYSQL,
        SQLDialect.POSTGRESQL,
    ),
)
def test_todate(sql_dialect: SQLDialect, request: pytest.FixtureRequest) -> None:
    sql_connection_config = request.getfixturevalue(f"{sql_dialect}_connection_config")

    if sql_dialect == SQLDialect.MYSQL:
        date_format = "%d/%m/%Y"
    else:
        date_format = "DD/MM/YYYY"

    sql_query_definition = SQLQueryDefinition(
        connection={
            "dialect": sql_dialect.value,
            "config": sql_connection_config,
        },
        pipeline={
            "steps": [
                {"name": "domain", "domain": "logins2"},
                {"name": "todate", "column": "login_text", "format": date_format},
            ],
        },
    )
    preview_query = PreviewQuery(
        query_def=sql_query_definition,
        tables=ALL_TEST_TABLES,
    )
    response = client.post("/preview", json=preview_query.dict())
    assert response.status_code == 200
    assert response.json() == [
        {"username": "Eric", "login_text": "2021-05-09", "type": None},
        {"username": "Chiara", "login_text": "2021-05-08", "type": None},
        {"username": "Pikachu", "login_text": "2020-01-01", "type": "Electric"},
        {"username": "Bulbi", "login_text": "2019-01-01", "type": "Grass/Poison"},
    ]
