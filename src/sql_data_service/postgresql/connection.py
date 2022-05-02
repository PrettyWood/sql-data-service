import asyncpg

from .models import PostgreSQLConnectionConfig


async def get_connection(postgresql_config: PostgreSQLConnectionConfig) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=postgresql_config.host,
        port=postgresql_config.port,
        user=postgresql_config.user,
        password=postgresql_config.password,
        database=postgresql_config.database,
    )
