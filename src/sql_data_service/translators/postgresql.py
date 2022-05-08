from pypika import PostgreSQLQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator


class PostgreSQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.POSTGRESQL
    QUERY_CLS = PostgreSQLQuery


SQLTranslator.register(PostgreSQLTranslator)
