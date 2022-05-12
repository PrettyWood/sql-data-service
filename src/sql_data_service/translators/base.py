from abc import ABC
from dataclasses import dataclass, field

# from typing_extensions import Self
from typing import TYPE_CHECKING, Any, Literal, Mapping, Sequence, TypeVar, cast

from pypika import Criterion, Field, JoinType, Order, Query, Schema, Table, functions

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


if TYPE_CHECKING:
    from typing import TypedDict

    from pypika.queries import QueryBuilder

    class SingleFilterCondition(TypedDict):
        column: str
        operator: Literal[
            "eq",
            "ne",
            "gt",
            "ge",
            "lt",
            "le",
            "in",
            "nin",
            "matches",
            "notmatches",
            "isnull",
            "notnull",
        ]
        value: Any

    class OrFilterCondition(TypedDict):
        or_: list["FilterCondition"]  # type: ignore

    class AndFilterCondition(TypedDict):
        and_: list["FilterCondition"]  # type: ignore

    FilterCondition = SingleFilterCondition | OrFilterCondition | AndFilterCondition  # type: ignore

    class SortColumn(TypedDict):
        column: str
        order: Literal["asc", "desc"]

    AggregationFunction = Literal["sum"]

    class Aggregation(TypedDict):
        new_columns: list[str]
        agg_function: AggregationFunction
        columns: list[str]


@dataclass
class QueryInfos:
    distinct_on: list[Field] = field(default_factory=list)
    selected: list[Field] = field(default_factory=list)
    from_: Table = Table("")
    wheres: list[Criterion] = field(default_factory=list)
    groupbys: list[Field] = field(default_factory=list)
    orders: dict[str, str] = field(default_factory=dict)
    limit: int | None = None
    sub_queries: dict[str, Query] = field(default_factory=dict)
    joins: list[tuple[Table, JoinType, tuple[str, ...]]] = field(default_factory=list)


class SQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query

    def __init__(
        self: Self,
        *,
        tables_columns: Mapping[str, Sequence[str]] | None = None,
        db_schema: str | None = None,
    ) -> None:
        self._tables_columns: Mapping[str, Sequence[str]] = tables_columns or {}
        self._db_schema: Schema | None = Schema(db_schema) if db_schema is not None else None
        self._query_infos = QueryInfos()

    def __init_subclass__(cls) -> None:
        ALL_TRANSLATORS[cls.DIALECT] = cls

    def get_query(self: Self) -> "QueryBuilder":
        query: "QueryBuilder" = self.QUERY_CLS

        for sub_query_alias, sub_query in self._query_infos.sub_queries.items():
            query = query.with_(sub_query, sub_query_alias)

        query = query.from_(self._query_infos.from_).select(*self._query_infos.selected)

        if hasattr(query.__class__, "distinct_on"):
            query = query.distinct_on(*self._query_infos.distinct_on)

        query = query.where(Criterion.all(self._query_infos.wheres))

        query = query.groupby(*self._query_infos.groupbys)

        for col, order in self._query_infos.orders.items():
            query = query.orderby(col, order=order)

        if self._query_infos.limit is not None:
            query = query.limit(self._query_infos.limit)

        for table, join_type, on_fields in self._query_infos.joins:
            query = query.join(table, join_type).on_field(*on_fields)

        return query

    def get_query_str(self: Self) -> str:
        query_str: str = self.get_query().get_sql()
        return query_str

    # All other methods implement step from https://weaverbird.toucantoco.com/docs/steps/,
    # the name of the method being the name of the step and the kwargs the rest of the params
    def argmax(self: Self, *, column: str, groups: Sequence[str]) -> Self:
        return self.top(rank_on=column, limit=1, sort="desc", groups=groups)

    def argmin(self: Self, *, column: str, groups: Sequence[str]) -> Self:
        return self.top(rank_on=column, limit=1, sort="asc", groups=groups)

    def aggregate(
        self: Self,
        *,
        on: Sequence[str],
        aggregations: Sequence["Aggregation"],
        keep_original_granularity: bool = False,
    ) -> Self:
        raise NotImplementedError(f"[{self.DIALECT}] aggregate is not implemented")

    def domain(self: Self, *, domain: str) -> Self:
        if self._db_schema is not None:
            self._query_infos.from_ = getattr(self._db_schema, domain)
        else:
            self._query_infos.from_ = Table(domain)
        try:
            self.select(columns=self._tables_columns[domain])
        except KeyError:
            self._query_infos.selected = ["*"]
        return self

    def delete(self: Self, *, columns: Sequence[str]) -> Self:
        self._query_infos.selected = [
            col_field for col_field in self._query_infos.selected if col_field.name not in columns
        ]
        return self

    def _get_single_condition_criterion(
        self: Self, condition: "SingleFilterCondition"
    ) -> Criterion:
        column_field: Field = getattr(self._query_infos.from_, condition["column"])

        match condition["operator"]:
            case "eq" | "ne" | "gt" | "ge" | "lt" | "le":
                import operator

                op = getattr(operator, condition["operator"])
                return op(column_field, condition["value"])
            case "in":
                return column_field.isin(condition["value"])
            case "nin":
                return column_field.notin(condition["value"])
            case "matches":
                return column_field.regexp(condition["value"])
            case "notmatches":
                return column_field.regexp(condition["value"]).negate()
            case "isnull":
                return column_field.isnull()
            case "notnull":
                return column_field.isnotnull()
            case _:  # pragma: no cover
                raise KeyError(f"Operator {condition['operator']!r} does not exist")

    def _get_filter_criterion(self: Self, condition: "FilterCondition") -> Criterion:
        match tuple(condition):
            case ("or_",):
                return Criterion.any(
                    (
                        self._get_filter_criterion(condition)
                        for condition in cast("OrFilterCondition", condition)["or_"]
                    )
                )
            case ("and_",):
                return Criterion.all(
                    (
                        self._get_filter_criterion(condition)
                        for condition in cast("AndFilterCondition", condition)["and_"]
                    )
                )
            case _:
                return self._get_single_condition_criterion(
                    cast("SingleFilterCondition", condition)
                )

    def filter(self: Self, *, condition: "FilterCondition") -> Self:
        self._query_infos.wheres.append(self._get_filter_criterion(condition))
        return self

    def lowercase(self: Self, *, column: str) -> Self:
        col_aliases = [c.alias or c.name for c in self._query_infos.selected]
        col_real_names = [c.name for c in self._query_infos.selected]
        idx = col_aliases.index(column)
        column_field: Field = getattr(self._query_infos.from_, col_real_names[idx])
        self._query_infos.selected[idx] = functions.Lower(column_field).as_(col_aliases[idx])
        return self

    def rename(self: Self, *, to_rename: tuple[str, str]) -> Self:
        for col_field in self._query_infos.selected:
            if col_field.name == to_rename[0]:
                col_field.alias = to_rename[1]
        return self

    def select(self: Self, *, columns: Sequence[str]) -> Self:
        self._query_infos.selected = [Field(col_name) for col_name in columns]
        return self

    def sort(self: Self, *, columns: Sequence["SortColumn"]) -> Self:
        for sorted_cols in columns:
            col_to_sort, order = sorted_cols["column"], sorted_cols["order"]
            self._query_infos.orders[col_to_sort] = (
                Order.desc if order.lower() == "desc" else Order.asc
            )
        return self

    def _top_with_groups(
        self: Self, rank_on: str, limit: int, order: Order, groups: Sequence[str]
    ) -> None:
        raise NotImplementedError(f"[{self.DIALECT}] top is not implemented with groups")

    def top(
        self: Self,
        *,
        rank_on: str,
        limit: int,
        sort: Literal["asc", "desc"],
        groups: Sequence[str],
    ) -> Self:
        order = Order.desc if sort == "desc" else Order.asc

        if groups:
            self._top_with_groups(rank_on, limit, order, groups)
        else:
            self._query_infos.orders[rank_on] = order
            self._query_infos.limit = limit

        return self

    def uniquegroups(self: Self, *, on: Sequence[str]) -> Self:
        raise NotImplementedError(f"[{self.DIALECT}] uniquegroups is not implemented")

    def uppercase(self: Self, *, column: str) -> Self:
        col_aliases = [c.alias or c.name for c in self._query_infos.selected]
        col_real_names = [c.name for c in self._query_infos.selected]
        idx = col_aliases.index(column)
        column_field: Field = getattr(self._query_infos.from_, col_real_names[idx])
        self._query_infos.selected[idx] = functions.Upper(column_field).as_(col_aliases[idx])
        return self


def get_aggregate_function(agg_function: "AggregationFunction") -> functions.AggregateFunction:
    match agg_function:
        case "sum":
            return functions.Sum
        case _:
            raise NotImplementedError(f"Aggregation for {agg_function!r} is not yet implemented")
