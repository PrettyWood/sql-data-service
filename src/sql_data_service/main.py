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
    return []
