from typing import Any

import asyncpg

from sql_data_service.dialects import SQLDialect
from sql_data_service.models.postgresql import PostgreSQLConnectionConfig

from .base import SQLExecutor


class PostgreSQLExecutor(SQLExecutor):
    DIALECT = SQLDialect.POSTGRESQL

    def __init__(self, conn_config: PostgreSQLConnectionConfig) -> None:
        self.conn_config = conn_config

    async def execute(self, sql_query: str) -> list[dict[str, Any]]:
        conn = await get_connection(self.conn_config)
        records = await conn.fetch(sql_query)
        await conn.close()
        return [dict(r) for r in records]

    async def get_all_columns(self, table_name: str) -> list[str]:
        records = await self.execute(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        )
        return [r["column_name"] for r in records]


SQLExecutor.register(PostgreSQLExecutor)


async def get_connection(postgresql_config: PostgreSQLConnectionConfig) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=postgresql_config.host,
        port=postgresql_config.port,
        user=postgresql_config.user,
        password=postgresql_config.password,
        database=postgresql_config.database,
    )
