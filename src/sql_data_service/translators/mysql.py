from typing import TYPE_CHECKING, TypeVar

from pypika import Field, Table, functions
from pypika.dialects import MySQLQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, StepTable

Self = TypeVar("Self", bound="SQLTranslator")

if TYPE_CHECKING:
    from pypika.queries import QueryBuilder
    from weaverbird.pipeline.steps import FromdateStep, ToDateStep

    from .base import WeaverbirdCastType


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery
    # https://www.w3schools.com/sql/func_mysql_cast.asp
    DATA_TYPE_MAPPING: dict["WeaverbirdCastType", str] = {
        "integer": "UNSIGNED",
        "float": "DECIMAL",
        "text": "CHAR",
        "date": "DATE",
        "boolean": "SIGNED",
    }

    def fromdate(
        self: Self, *, step: "FromdateStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_field: Field = Table(table.name)[step.column]
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
            *(c for c in table.columns if c != step.column),
            DateFormat(col_field, step.format).as_(step.column),
        )
        return query, StepTable(columns=table.columns)

    def todate(
        self: Self, *, step: "ToDateStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_field: Field = Table(table.name)[step.column]

        if step.format is not None:
            date_selection = StrToDate(col_field, step.format)
        else:
            date_selection = functions.Cast(col_field, self.DATA_TYPE_MAPPING["date"])

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
            *(c for c in table.columns if c != step.column),
            date_selection.as_(col_field.name),
        )
        return query, StepTable(columns=table.columns)


SQLTranslator.register(MySQLTranslator)


class DateFormat(functions.Function):  # type: ignore[misc]
    def __init__(self, term: str | Field, date_format: str, alias: str | None = None) -> None:
        super().__init__("DATE_FORMAT", term, date_format, alias=alias)


class StrToDate(functions.Function):  # type: ignore[misc]
    def __init__(self, term: str | Field, date_format: str, alias: str | None = None) -> None:
        super().__init__("STR_TO_DATE", term, date_format, alias=alias)
