from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field
from weaverbird.pipeline import PipelineWithVariables

SQLDialect = Literal["mysql", "postgresql"]
DataRows = list[dict[str, Any]]


# MySQL
###############################################################################
class MySQLConnectionConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str | None = None
    password: str | None = None
    database: str | None = None
    charset: str | None = None
    connect_timeout: int | None = None


class MySQLConnection(BaseModel):
    dialect: Literal["mysql"]
    config: MySQLConnectionConfig


# PostgreSQL
###############################################################################
class PostgreSQLConnectionConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5432
    user: str | None = None
    password: str | None = None
    database: str | None = None
    charset: str | None = None
    connect_timeout: int | None = None


class PostgreSQLConnection(BaseModel):
    dialect: Literal["postgresql"]
    config: PostgreSQLConnectionConfig


###############################################################################

SQLConnection = Annotated[MySQLConnection | PostgreSQLConnection, Field(discriminator="dialect")]


class SQLQueryDefinition(BaseModel):
    connection: SQLConnection
    pipeline: PipelineWithVariables
