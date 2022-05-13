from typing import Any

import aiomysql

from sql_data_service.dialects import SQLDialect
from sql_data_service.models.mysql import MySQLConnectionConfig

from .base import SQLExecutor


class MySQLExecutor(SQLExecutor):
    DIALECT = SQLDialect.MYSQL

    def __init__(self, conn_config: MySQLConnectionConfig) -> None:
        self.conn_config = conn_config

    async def execute(self, sql_query: str) -> list[dict[str, Any]]:
        conn = await get_connection(self.conn_config)

        async with conn.cursor(aiomysql.DictCursor) as cur:
            assert isinstance(cur, aiomysql.Cursor)
            await cur.execute(sql_query)
            dict_records: list[dict[str, Any]] = await cur.fetchall()

        conn.close()
        return dict_records

    async def get_all_columns(self, table_name: str) -> list[str]:
        records = await self.execute(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        )
        return [r["COLUMN_NAME"] for r in records]


SQLExecutor.register(MySQLExecutor)


async def get_connection(mysql_config: MySQLConnectionConfig) -> aiomysql.Connection:
    return await aiomysql.connect(
        host=mysql_config.host,
        port=mysql_config.port,
        user=mysql_config.user,
        password=mysql_config.password,
        db=mysql_config.database,
    )
