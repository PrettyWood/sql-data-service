from typing import cast

import aiomysql
from pypika import MySQLQuery
from weaverbird.pipeline import PipelineWithVariables
from weaverbird.pipeline.steps import DomainStep


async def translate_pipeline_mysql(
    pipeline: PipelineWithVariables, conn: aiomysql.Connection
) -> str:
    query = MySQLQuery()
    metadata = {}  # domain -> metadata on all columns

    for step in pipeline.steps:
        match step.name:
            case "domain":
                assert isinstance(step, DomainStep)
                all_columns = await get_all_columns(step.domain, conn)
                metadata[step.domain] = {col_name: True for col_name in all_columns}
                query = query.from_(step.domain).select(*all_columns)
            case _:  # pragma: no cover
                raise NotImplementedError("Step {step.name!r} is not yet implemented")

    return cast(str, query.get_sql())


async def get_all_columns(table: str, conn: aiomysql.Connection) -> list[str]:
    get_columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """

    cur = await conn.cursor()
    await cur.execute(get_columns_query)
    column_records = await cur.fetchall()
    await cur.close()

    return [r[0] for r in column_records]
