from abc import ABC
from dataclasses import dataclass, field

# from typing_extensions import Self
from typing import TYPE_CHECKING, Mapping, Sequence, TypeVar, cast

from pypika import Criterion, Field, Order, Query, Table, functions

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


if TYPE_CHECKING:
    from typing import Any, Literal, TypedDict

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


@dataclass
class QueryInfos:
    selected: list[Field] = field(default_factory=list)
    from_: Table = Table("")
    wheres: list[Criterion] = field(default_factory=list)
    orders: dict[str, str] = field(default_factory=dict)


class SQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query

    def __init__(self: Self, *, tables_columns: Mapping[str, Sequence[str]] | None = None) -> None:
        self._tables_columns: Mapping[str, Sequence[str]] = tables_columns or {}
        self._query_infos = QueryInfos()

    def __init_subclass__(cls) -> None:
        ALL_TRANSLATORS[cls.DIALECT] = cls

    def get_query(self: Self) -> str:
        query = self.QUERY_CLS().from_(self._query_infos.from_).select(*self._query_infos.selected)

        query = query.where(Criterion.all(self._query_infos.wheres))

        for col, order in self._query_infos.orders.items():
            query = query.orderby(col, order=order)

        query_str: str = query.get_sql()
        return query_str

    # All other methods implement step from https://weaverbird.toucantoco.com/docs/steps/,
    # the name of the method being the name of the step and the kwargs the rest of the params
    def domain(self: Self, *, domain: str) -> Self:
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

    def uppercase(self: Self, *, column: str) -> Self:
        col_aliases = [c.alias or c.name for c in self._query_infos.selected]
        col_real_names = [c.name for c in self._query_infos.selected]
        idx = col_aliases.index(column)
        column_field: Field = getattr(self._query_infos.from_, col_real_names[idx])
        self._query_infos.selected[idx] = functions.Upper(column_field).as_(col_aliases[idx])
        return self
