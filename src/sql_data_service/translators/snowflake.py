from pypika.dialects import SnowflakeQuery

from sql_data_service.dialects import SQLDialect
from sql_data_service.operators import FromDateOp, RegexOp, ToDateOp

from .base import DataTypeMapping, SQLTranslator


class SnowflakeTranslator(SQLTranslator):
    DIALECT = SQLDialect.SNOWFLAKE
    QUERY_CLS = SnowflakeQuery
    DATA_TYPE_MAPPING = DataTypeMapping(
        boolean="BOOLEAN",
        date="DATE",
        float="DOUBLE PRECISION",
        integer="INTEGER",
        text="TEXT",
    )
    SUPPORT_DISTINCT_ON = False
    SUPPORT_ROW_NUMBER = True
    SUPPORT_SPLIT_PART = True
    FROM_DATE_OP = FromDateOp.TO_CHAR
    REGEXP_OP = RegexOp.REGEXP
    TO_DATE_OP = ToDateOp.TO_DATE


SQLTranslator.register(SnowflakeTranslator)
