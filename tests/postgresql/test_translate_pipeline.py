import asyncpg
import pytest
import pytest_asyncio
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.postgresql.connection import get_connection
from sql_data_service.postgresql.models import PostgreSQLConnectionConfig
from sql_data_service.postgresql.translate import translate_pipeline_postgresql


@pytest_asyncio.fixture
async def postgresql_connection(
    postgresql_connection_config: PostgreSQLConnectionConfig,
) -> asyncpg.Connection:
    return await get_connection(postgresql_connection_config)


@pytest.mark.asyncio
async def test_translate_pipeline(postgresql_connection: asyncpg.Connection) -> None:
    pipeline = PipelineWithVariables(steps=[{"name": "domain", "domain": "users"}])
    query = await translate_pipeline_postgresql(pipeline, postgresql_connection)
    assert query == 'SELECT "username","age","city" FROM "users"'
