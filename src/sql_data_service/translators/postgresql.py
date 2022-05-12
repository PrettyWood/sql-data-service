from typing import TYPE_CHECKING, Any, Sequence, TypeVar

from pypika import Field, Table
from pypika.dialects import PostgreSQLQuery
from pypika.enums import Comparator, JoinType, Order
from pypika.terms import AnalyticFunction, BasicCriterion, Criterion

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, get_aggregate_function

Self = TypeVar("Self", bound="PostgreSQLTranslator")

if TYPE_CHECKING:
    from .base import Aggregation, SingleFilterCondition


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

    def aggregate(
        self: Self,
        *,
        on: Sequence[str],
        aggregations: Sequence["Aggregation"],
        keep_original_granularity: bool = False,
    ) -> Self:

        agg_selected: list[Field] = []
        for aggregation in aggregations:
            agg_fn = get_aggregate_function(aggregation["agg_function"])
            for i, column in enumerate(aggregation["columns"]):
                column_field: Field = getattr(self._query_infos.from_, column)
                new_agg_col = agg_fn(column_field).as_(aggregation["new_columns"][i])
                agg_selected.append(new_agg_col)

        if keep_original_granularity:
            self._query_infos.sub_queries["__original__"] = self.get_query()
            self._query_infos.sub_queries["__aggregated__"] = (
                self.QUERY_CLS.from_(self._query_infos.from_)
                .select(*on, *agg_selected)
                .groupby(*on)
            )

            left_table = Table("__original__")
            right_table = Table("__aggregated__")
            all_agg_col_names = [x for agg in aggregations for x in agg["new_columns"]]
            self._query_infos.from_ = left_table
            self._query_infos.selected = [
                *(Field(name=f.name, table=left_table) for f in self._query_infos.selected),
                *(Field(name=col_name, table=right_table) for col_name in all_agg_col_names),
            ]
            self._query_infos.joins.append((right_table, JoinType.left, tuple(on)))
        else:
            self._query_infos.selected = [*on, *agg_selected]
            self._query_infos.groupbys = list(on)
            self._query_infos.orders = {k: Order.asc for k in on}

        return self

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

    def uniquegroups(self: Self, *, on: Sequence[str]) -> Self:
        self._query_infos.distinct_on = list(on)
        return self


SQLTranslator.register(PostgreSQLTranslator)
