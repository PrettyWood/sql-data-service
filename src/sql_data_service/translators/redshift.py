from pypika.dialects import RedshiftQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator


class RedshiftQueryTranslator(SQLTranslator):
    DIALECT = SQLDialect.REDSHIFT
    QUERY_CLS = RedshiftQuery


SQLTranslator.register(RedshiftQueryTranslator)
