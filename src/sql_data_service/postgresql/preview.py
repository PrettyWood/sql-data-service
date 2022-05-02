from typing import Any

from weaverbird.pipeline import PipelineStepWithVariables

from .connection import get_connection
from .models import PostgreSQLConnectionConfig
from .translate import translate_pipeline_postgresql


async def get_preview_postgresql(
    postgresql_config: PostgreSQLConnectionConfig, pipeline: PipelineStepWithVariables
) -> list[dict[str, Any]]:
    conn = await get_connection(postgresql_config)
    sql_query = await translate_pipeline_postgresql(pipeline, conn)
    records = await conn.fetch(sql_query)

    await conn.close()
    return [dict(r) for r in records]
