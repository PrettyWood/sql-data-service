from pypika import PostgreSQLQuery

from sql_data_service.dialects import SQLDialect

from .base import BaseSQLTranslator


class PostgreSQLTranslator(BaseSQLTranslator):
    DIALECT = SQLDialect.POSTGRESQL
    QUERY_CLS = PostgreSQLQuery


BaseSQLTranslator.register(PostgreSQLTranslator)
