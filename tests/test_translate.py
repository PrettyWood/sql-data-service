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
        sql_dialect=SQLDialect.POSTGRESQL,
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
    )
    response = client.post("/translate", json=translation_query.dict())
    assert response.status_code == 200
    assert (
        response.json()
        == 'SELECT "username","age","city" FROM "users" ORDER BY "age" ASC,"username" DESC'
    )
