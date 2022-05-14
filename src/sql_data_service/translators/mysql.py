from typing import TYPE_CHECKING, TypeVar

from pypika import Field, Table, functions
from pypika.dialects import MySQLQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, StepTable

Self = TypeVar("Self", bound="SQLTranslator")

if TYPE_CHECKING:
    from pypika.queries import QueryBuilder
    from weaverbird.pipeline.steps import FromdateStep


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery

    def fromdate(
        self: Self, *, step: "FromdateStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_field: Field = Table(table.name)[step.column]
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
            *(c for c in table.columns if c != step.column),
            DateFormat(col_field, step.format).as_(step.column),
        )
        return query, StepTable(columns=table.columns)


SQLTranslator.register(MySQLTranslator)


class DateFormat(functions.Function):  # type: ignore[misc]
    def __init__(self, term: str | Field, date_format: str, alias: str | None = None) -> None:
        super().__init__("DATE_FORMAT", term, date_format, alias=alias)
