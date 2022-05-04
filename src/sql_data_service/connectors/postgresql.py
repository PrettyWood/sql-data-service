import asyncpg

from sql_data_service.models import DataRows, PostgreSQLConnectionConfig

from .base import BaseSQLExecutor


class PostgreSQLExecutor(BaseSQLExecutor):
    def __init__(self, conn_config: PostgreSQLConnectionConfig) -> None:
        self.conn_config = conn_config

    async def execute(self, sql_query: str) -> DataRows:
        conn = await get_connection(self.conn_config)
        records = await conn.fetch(sql_query)
        await conn.close()
        return [dict(r) for r in records]


BaseSQLExecutor.register(PostgreSQLExecutor)


async def get_connection(postgresql_config: PostgreSQLConnectionConfig) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=postgresql_config.host,
        port=postgresql_config.port,
        user=postgresql_config.user,
        password=postgresql_config.password,
        database=postgresql_config.database,
    )
