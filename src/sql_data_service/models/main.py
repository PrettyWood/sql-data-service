from typing import Annotated, Literal

from pydantic import BaseModel, Field
from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect

from .mysql import MySQLConnectionConfig
from .postgresql import PostgreSQLConnectionConfig


class BaseSQLQueryDefinition(BaseModel):
    dialect: SQLDialect
    pipeline: PipelineWithVariables


class MySQLQueryDefinition(BaseSQLQueryDefinition):
    dialect: Literal[SQLDialect.MYSQL] = SQLDialect.MYSQL
    connection: MySQLConnectionConfig


class PostgresSQLQueryDefinition(BaseSQLQueryDefinition):
    dialect: Literal[SQLDialect.POSTGRESQL] = SQLDialect.POSTGRESQL
    connection: PostgreSQLConnectionConfig


SQLQueryDefinition = Annotated[
    MySQLQueryDefinition | PostgresSQLQueryDefinition, Field(discriminator="dialect")
]
