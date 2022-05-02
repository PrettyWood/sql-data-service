from typing import Any

from fastapi import FastAPI

from . import __version__
from .models import SQLQueryDefinition

app = FastAPI()


@app.get("/")
def get_status() -> dict[str, str]:
    return {"status": "OK", "version": __version__}


@app.post("/preview")
async def get_preview(query_def: SQLQueryDefinition) -> list[dict[str, Any]]:
    match query_def.connection.dialect:
        case "postgresql":
            from .postgresql.models import PostgreSQLConnectionConfig
            from .postgresql.preview import get_preview_postgresql

            assert isinstance(query_def.connection.config, PostgreSQLConnectionConfig)
            return await get_preview_postgresql(query_def.connection.config, query_def.pipeline)
        case "mysql":  # pragma: no cover
            from .mysql.models import MySQLConnectionConfig
            from .mysql.preview import get_preview_mysql

            assert isinstance(query_def.connection.config, MySQLConnectionConfig)
            return await get_preview_mysql(query_def.connection.config, query_def.pipeline)
