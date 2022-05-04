# import asyncio

# import pytest
# import pytest_asyncio

# from sql_data_service.models import MySQLConnectionConfig, PostgreSQLConnectionConfig

# # MYSQL
# ###############################################################################


# @pytest.fixture
# def mysql_connection_config() -> MySQLConnectionConfig:
#     return MySQLConnectionConfig(
#         host="127.0.0.1",
#         port=3306,
#         user="pika_user",
#         password="pika_pw",
#         database="pika_db",
#     )


# @pytest_asyncio.fixture
# async def is_mysql_ready(mysql_connection_config: MySQLConnectionConfig) -> bool:
#     from sql_data_service.mysql.connection import get_connection

#     while (retry := 0) < 20:
#         try:
#             await get_connection(mysql_connection_config)
#             return True
#         except Exception:
#             print("Could not connect to MySQL server. Retrying...")
#             retry += 1
#             await asyncio.sleep(1)

#     return False


# # POSTGRESQL
# ###############################################################################


# @pytest.fixture
# def postgresql_connection_config() -> PostgreSQLConnectionConfig:
#     return PostgreSQLConnectionConfig(
#         host="127.0.0.1",
#         port=5432,
#         user="pika_user",
#         password="pika_pw",
#         database="pika_db",
#     )


# @pytest_asyncio.fixture
# async def is_postgresql_ready(postgresql_connection_config: PostgreSQLConnectionConfig) -> bool:
#     from sql_data_service.postgresql.connection import get_connection

#     while (retry := 0) < 20:
#         try:
#             await get_connection(postgresql_connection_config)
#             return True
#         except Exception:
#             print("Could not connect to PostgreSQL server. Retrying...")
#             retry += 1
#             await asyncio.sleep(1)

#     return False
