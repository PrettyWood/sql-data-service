import asyncpg
from pypika import Query
from weaverbird.pipeline import PipelineWithVariables
from weaverbird.pipeline.steps import DomainStep


async def translate_pipeline_sql(
    pipeline: PipelineWithVariables, conn: asyncpg.Connection, dialect="postgres"
) -> str:
    query = Query()
    metadata = {}  # domain -> metadata on all columns

    for step in pipeline.steps:
        match step.name:
            case "domain":
                step: DomainStep
                all_columns = await get_all_columns(step.domain, conn)
                metadata[step.domain] = {col_name: True for col_name in all_columns}
                query = query.from_(step.domain).select(*all_columns)
            case _:  # pragma: no cover
                raise NotImplementedError("Step {step.name!r} is not yet implemented")

    return query.get_sql()


async def get_all_columns(table: str, conn: asyncpg.Connection) -> list[str]:
    get_columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """
    column_records = await conn.fetch(get_columns_query)
    return [r[0] for r in column_records]
