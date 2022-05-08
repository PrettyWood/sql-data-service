import asyncio

from sql_data_service.connectors.mysql import MySQLExecutor
from sql_data_service.connectors.postgresql import PostgreSQLExecutor
from sql_data_service.models.mysql import MySQLConnectionConfig
from sql_data_service.models.postgresql import PostgreSQLConnectionConfig
from sql_data_service.translators.mysql import MySQLTranslator
from sql_data_service.translators.postgresql import PostgreSQLTranslator

# MYSQL
###################################################################

MYSQL_CONN_CONFIG = MySQLConnectionConfig(
    host="127.0.0.1",
    port=3306,
    user="pika_user",
    password="pika_pw",
    database="pika_db",
)


async def test_mysql() -> None:
    mysql_executor = MySQLExecutor(MYSQL_CONN_CONFIG)
    all_users_columns = await mysql_executor.get_all_columns("users")

    sql_query = (
        MySQLTranslator(tables_columns={"users": all_users_columns})
        .domain(table_name="users")
        .get_query()
    )
    res = await mysql_executor.execute(sql_query)
    assert res == [
        {"username": "Eric", "age": 30, "city": "Paris"},
        {"username": "Chiara", "age": 31, "city": "Firenze"},
        {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
    ]


asyncio.run(test_mysql())


# POSTGRESQL
###################################################################

POSTGRESQL_CONN_CONFIG = PostgreSQLConnectionConfig(
    host="127.0.0.1",
    port=5432,
    user="pika_user",
    password="pika_pw",
    database="pika_db",
)


async def test_postgresql() -> None:
    postgresql_executor = PostgreSQLExecutor(POSTGRESQL_CONN_CONFIG)
    all_users_columns = await postgresql_executor.get_all_columns("users")

    sql_query = (
        PostgreSQLTranslator(tables_columns={"users": all_users_columns})
        .domain(table_name="users")
        .get_query()
    )
    res = await postgresql_executor.execute(sql_query)
    assert res == [
        {"username": "Eric", "age": 30, "city": "Paris"},
        {"username": "Chiara", "age": 31, "city": "Firenze"},
        {"username": "Pikachu", "age": 7, "city": "Bourg Palette"},
    ]


asyncio.run(test_postgresql())
