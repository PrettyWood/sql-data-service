import asyncpg
import pytest
import pytest_asyncio
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.translate_pipeline_sql import translate_pipeline_sql


@pytest_asyncio.fixture
async def postgres_conn():
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=5432,
        user="postgres",
        password="pikapika",
    )
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_translate_pipeline(postgres_conn):
    pipeline = PipelineWithVariables(steps=[{"name": "domain", "domain": "table_test"}])
    query = await translate_pipeline_sql(pipeline, postgres_conn)
    assert query == 'SELECT "username","age","city" FROM "table_test"'
