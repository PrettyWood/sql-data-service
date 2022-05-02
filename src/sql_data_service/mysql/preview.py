from typing import Any

import aiomysql
from weaverbird.pipeline import PipelineStepWithVariables

from .connection import get_connection
from .models import MySQLConnectionConfig
from .translate import translate_pipeline_mysql


async def get_preview_mysql(
    mysql_config: MySQLConnectionConfig, pipeline: PipelineStepWithVariables
) -> list[dict[str, Any]]:
    conn = await get_connection(mysql_config)
    sql_query = await translate_pipeline_mysql(pipeline, conn)

    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(sql_query)
        dict_records: list[dict[str, Any]] = await cur.fetchall()

    conn.close()
    return dict_records
