# from typing import Literal

# import pytest
# from fastapi.testclient import TestClient

# from sql_data_service.main import app

# client = TestClient(app)


# @pytest.mark.usefixtures(
#     "is_mysql_ready",
#     "is_postgresql_ready",
# )
# @pytest.mark.parametrize(
#     "sql_dialect",
#     [
#         "mysql",
#         "postgresql",
#     ],
# )
# def test_get_preview_mysql(
#     sql_dialect: Literal["mysql", "postgresql"],
#     request: pytest.FixtureRequest,
# ) -> None:
#     sql_connection_config = request.getfixturevalue(f"{sql_dialect}_connection_config")
#     response = client.post(
#         "/preview",
#         json={
#             "connection": {
#                 "dialect": sql_dialect,
#                 "config": sql_connection_config.dict(),
#             },
#             "pipeline": {
#                 "steps": [{"name": "domain", "domain": "users"}],
#             },
#         },
#     )
#     assert response.status_code == 200
#     assert response.json() == [
#         {"username": "Eric", "age": 30, "city": "Paris"},
#         {"username": "Chiara", "age": 31, "city": "Firenze"},
#         {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
#     ]
