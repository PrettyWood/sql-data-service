from pypika.dialects import SnowflakeQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator


class SnowflakeTranslator(SQLTranslator):
    DIALECT = SQLDialect.SNOWFLAKE
    QUERY_CLS = SnowflakeQuery


SQLTranslator.register(SnowflakeTranslator)
