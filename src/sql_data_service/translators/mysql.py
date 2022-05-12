from typing import Sequence, TypeVar

from pypika.dialects import MySQLQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator

Self = TypeVar("Self", bound="SQLTranslator")


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery

    def uniquegroups(self: Self, *, on: Sequence[str]) -> Self:
        raise NotImplementedError()


SQLTranslator.register(MySQLTranslator)
