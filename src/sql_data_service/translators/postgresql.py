from typing import TYPE_CHECKING, Any, Sequence, TypeVar

from pypika import Field, Table
from pypika.dialects import PostgreSQLQuery
from pypika.enums import Comparator, Order
from pypika.terms import AnalyticFunction, BasicCriterion, Criterion

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator

Self = TypeVar("Self", bound="PostgreSQLTranslator")

if TYPE_CHECKING:
    from .base import SingleFilterCondition


class PostgreSQLMatching(Comparator):  # type: ignore[misc]
    similar_to = " SIMILAR TO "
    not_similar_to = " NOT SIMILAR TO "


class RowNumber(AnalyticFunction):  # type: ignore[misc]
    def __init__(self, **kwargs: Any) -> None:
        super().__init__("ROW_NUMBER", **kwargs)


def _compliant_regex(pattern: str) -> str:
    """
    Like LIKE, the SIMILAR TO operator succeeds only if its pattern matches the entire string;
    this is unlike common regular expression behavior wherethe pattern
    can match any part of the string
    (see https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-SIMILARTO-REGEXP)
    """
    return f"%{pattern}%"


class PostgreSQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.POSTGRESQL
    QUERY_CLS = PostgreSQLQuery

    def _get_single_condition_criterion(self, condition: "SingleFilterCondition") -> Criterion:
        column_field: Field = getattr(self._query_infos.from_, condition["column"])
        match condition["operator"]:
            case "matches":
                return BasicCriterion(
                    PostgreSQLMatching.similar_to,
                    column_field,
                    column_field.wrap_constant(_compliant_regex(condition["value"])),
                )
            case "notmatches":
                return BasicCriterion(
                    PostgreSQLMatching.not_similar_to,
                    column_field,
                    column_field.wrap_constant(_compliant_regex(condition["value"])),
                )
            case _:
                return super()._get_single_condition_criterion(condition)

    def _top_with_groups(
        self: Self, rank_on: str, limit: int, order: Order, groups: Sequence[str]
    ) -> None:
        import operator

        rank_on_field: Field = getattr(self._query_infos.from_, rank_on)
        groups_fields: list[Field] = [getattr(self._query_infos.from_, group) for group in groups]

        top_sub_query = self.QUERY_CLS.from_(self._query_infos.from_).select(
            *self._query_infos.selected,
            RowNumber().over(*groups_fields).orderby(rank_on_field, order=order),
        )
        top_table = Table("__top__")
        self._query_infos.sub_queries["__top__"] = top_sub_query
        self._query_infos.from_ = top_table
        self._query_infos.wheres = [operator.eq(top_table["row_number"], limit)]


SQLTranslator.register(PostgreSQLTranslator)
