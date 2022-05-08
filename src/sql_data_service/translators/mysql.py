from pypika import MySQLQuery

from sql_data_service.dialects import SQLDialect

from .base import BaseSQLTranslator


class MySQLTranslator(BaseSQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery


BaseSQLTranslator.register(MySQLTranslator)
