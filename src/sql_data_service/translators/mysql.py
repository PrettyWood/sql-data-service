from pypika.dialects import MySQLQuery

from sql_data_service.dialects import SQLDialect
from sql_data_service.operators import FromDateOp, RegexOp, ToDateOp

from .base import DataTypeMapping, SQLTranslator


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery
    DATA_TYPE_MAPPING = DataTypeMapping(
        boolean="SIGNED",
        date="DATE",
        float="DECIMAL",
        integer="UNSIGNED",
        text="CHAR",
    )
    SUPPORT_ROW_NUMBER = False
    SUPPORT_SPLIT_PART = False
    FROM_DATE_OP = FromDateOp.DATE_FORMAT
    REGEXP_OP = RegexOp.REGEXP
    TO_DATE_OP = ToDateOp.STR_TO_DATE


SQLTranslator.register(MySQLTranslator)
