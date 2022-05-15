from pypika.dialects import RedshiftQuery

from sql_data_service.dialects import SQLDialect
from sql_data_service.operators import FromDateOp, RegexOp, ToDateOp

from .base import DataTypeMapping, SQLTranslator


class GoogleBigQueryTranslator(SQLTranslator):
    DIALECT = SQLDialect.GOOGLEBIGQUERY
    QUERY_CLS = RedshiftQuery
    DATA_TYPE_MAPPING = DataTypeMapping(
        boolean="BOOLEAN",
        date="DATE",
        float="DOUBLE PRECISION",
        integer="INTEGER",
        text="TEXT",
    )
    SUPPORT_ROW_NUMBER = True
    SUPPORT_SPLIT_PART = False
    FROM_DATE_OP = FromDateOp.TO_CHAR
    REGEXP_OP = RegexOp.CONTAINS
    TO_DATE_OP = ToDateOp.PARSE_DATE


SQLTranslator.register(GoogleBigQueryTranslator)
