from typing import Annotated

from pydantic import BaseModel, Field
from weaverbird.pipeline import PipelineWithVariables

from .mysql.models import MySQLConnection, MySQLConnectionConfig
from .postgresql.models import PostgreSQLConnection, PostgreSQLConnectionConfig

__all__ = (
    "SQLQueryDefinition",
    "SQLConnection",
    "PipelineWithVariables",
    # mysql
    "MySQLConnection",
    "MySQLConnectionConfig",
    # postgresql
    "PostgreSQLConnection",
    "PostgreSQLConnectionConfig",
)

SQLConnection = Annotated[MySQLConnection | PostgreSQLConnection, Field(discriminator="dialect")]


class SQLQueryDefinition(BaseModel):
    connection: SQLConnection
    pipeline: PipelineWithVariables
