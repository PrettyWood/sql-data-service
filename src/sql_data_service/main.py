from dataclasses import dataclass
from typing import Any

import asyncpg
from fastapi import FastAPI
from pydantic import BaseModel, SecretStr
from weaverbird.pipeline import PipelineWithVariables

from . import __version__
from .translate_pipeline_sql import translate_pipeline_sql

app = FastAPI()


class PostgreSQLConnection(BaseModel):
    host: str
    port: int
    user: str | None = None
    password: SecretStr | None = None
    database: str | None = None
    charset: str | None = None
    connect_timeout: int | None = None


class PostgreSQLQueryDefinition(BaseModel):
    connection: PostgreSQLConnection
    pipeline: PipelineWithVariables


Datarows = list[dict[str, Any]]


@app.get("/")
def get_status():
    return {"status": "OK", "version": __version__}


@app.post("/preview")
async def get_preview(query_def: PostgreSQLQueryDefinition) -> Datarows:
    conn = await asyncpg.connect(
        host=query_def.connection.host,
        port=query_def.connection.port,
        user=query_def.connection.user,
        password=query_def.connection.password.get_secret_value(),
        database=query_def.connection.database,
    )

    sql_query = await translate_pipeline_sql(query_def.pipeline, conn, dialect="postgres")
    records = await conn.fetch(sql_query)

    await conn.close()
    return [dict(r) for r in records]
