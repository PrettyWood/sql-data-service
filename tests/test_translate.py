from fastapi.testclient import TestClient

from sql_data_service.app import TranslationQuery, app
from sql_data_service.dialects import SQLDialect

client = TestClient(app)


ALL_TABLES_COLUMNS = {
    "labels": ["Label", "Cartel", "Value"],
    "logins": ["username", "login", "type"],
    "users": ["username", "age", "city"],
}


def test_translate() -> None:
    translation_query = TranslationQuery(
        sql_dialect=SQLDialect.REDSHIFT,
        pipeline={
            "steps": [
                {"name": "domain", "domain": "users"},
                {
                    "name": "sort",
                    "columns": [
                        {"column": "age", "order": "asc"},
                        {"column": "username", "order": "desc"},
                    ],
                },
            ]
        },
        tables_columns=ALL_TABLES_COLUMNS,
        db_schema="AB123CD",
    )
    response = client.post("/translate", json=translation_query.dict())
    assert response.status_code == 200
    assert response.json() == (
        'WITH __step_0__ AS (SELECT "username","age","city" FROM "AB123CD"."users") ,'
        '__step_1__ AS (SELECT "username","age","city" FROM "__step_0__" ORDER BY "age" ASC,"username" DESC) '
        'SELECT * FROM "__step_1__"'
    )
