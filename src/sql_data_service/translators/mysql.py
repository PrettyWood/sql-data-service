from typing import TypeVar

from pypika.dialects import MySQLQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator

Self = TypeVar("Self", bound="SQLTranslator")


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery


SQLTranslator.register(MySQLTranslator)
