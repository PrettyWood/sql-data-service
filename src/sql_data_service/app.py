from typing import Any, Mapping, Sequence

from fastapi import FastAPI
from pydantic import BaseModel
from weaverbird.pipeline import PipelineWithVariables

from . import __version__
from .connectors import ALL_EXECUTORS
from .dialects import SQLDialect
from .models import SQLQueryDefinition
from .translate import translate_pipeline

app = FastAPI()


@app.get("/")
def get_status() -> dict[str, str]:
    return {"status": "OK", "version": __version__}


class TranslationQuery(BaseModel):
    sql_dialect: SQLDialect
    pipeline: PipelineWithVariables
    tables_columns: Mapping[str, Sequence[str]]


@app.post("/translate")
async def get_translation(translation_query: TranslationQuery) -> str:
    return translate_pipeline(
        sql_dialect=translation_query.sql_dialect,
        pipeline=translation_query.pipeline,
        tables_columns=translation_query.tables_columns,
    )


class PreviewQuery(BaseModel):
    query_def: SQLQueryDefinition
    tables: Sequence[str] | None = None


@app.post("/preview")
async def get_preview(preview_query: PreviewQuery) -> list[dict[str, Any]]:
    sql_dialect = preview_query.query_def.connection.dialect
    connection_config = preview_query.query_def.connection.config

    executor_cls = ALL_EXECUTORS[sql_dialect]
    executor = executor_cls(connection_config)

    tables_columns: dict[str, list[str]] = {}
    for table_name in preview_query.tables or []:
        tables_columns[table_name] = await executor.get_all_columns(table_name)

    sql_query = translate_pipeline(
        sql_dialect=sql_dialect,
        pipeline=preview_query.query_def.pipeline,
        tables_columns=tables_columns,
    )

    return await executor.execute(sql_query)
