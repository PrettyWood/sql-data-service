from typing import Annotated, Literal

from pydantic import BaseModel, Field
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect

from .mysql import MySQLConnectionConfig
from .postgresql import PostgreSQLConnectionConfig


class MySQLConnection(BaseModel):
    dialect: Literal[SQLDialect.MYSQL] = SQLDialect.MYSQL
    config: MySQLConnectionConfig


class PostgresSQLConnection(BaseModel):
    dialect: Literal[SQLDialect.POSTGRESQL] = SQLDialect.POSTGRESQL
    config: PostgreSQLConnectionConfig


SQLConnection = Annotated[MySQLConnection | PostgresSQLConnection, Field(discriminator="dialect")]


class SQLQueryDefinition(BaseModel):
    connection: SQLConnection
    pipeline: PipelineWithVariables


__all__ = (
    "SQLConnection",
    "SQLQueryDefinition",
    # mysql
    "MySQLConnectionConfig",
    "MySQLConnection",
    # postgres
    "PostgreSQLConnectionConfig",
    "PostgresSQLConnection",
)
