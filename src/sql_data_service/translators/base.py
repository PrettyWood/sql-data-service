from abc import ABC
from dataclasses import dataclass, field

# from typing_extensions import Self
from typing import Mapping, Sequence, TypeVar

from pypika import Field, Order, Query

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


@dataclass
class QueryInfos:
    selected: list[Field] = field(default_factory=list)
    from_: str = ""
    order_by: dict[str, str] = field(default_factory=dict)


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
        for col, order in self._query_infos.order_by.items():
            query = query.orderby(col, order=order)

        query_str: str = query.get_sql()
        return query_str

    # All other methods implement step from https://weaverbird.toucantoco.com/docs/steps/,
    # the name of the method being the name of the step and the kwargs the rest of the params
    def domain(self: Self, *, domain: str) -> Self:
        self._query_infos.from_ = domain
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

    def rename(self: Self, *, to_rename: tuple[str, str]) -> Self:
        for col_field in self._query_infos.selected:
            if col_field.name == to_rename[0]:
                col_field.alias = to_rename[1]
        return self

    def select(self: Self, *, columns: Sequence[str]) -> Self:
        self._query_infos.selected = [Field(col_name) for col_name in columns]
        return self

    # columns = [{"column": "age", "order": "asc"}]
    def sort(self: Self, *, columns: Sequence[dict[str, str]]) -> Self:
        for sorted_cols in columns:
            col_to_sort, order = sorted_cols["column"], sorted_cols["order"]
            self._query_infos.order_by[col_to_sort] = (
                Order.desc if order.lower() == "desc" else Order.asc
            )
        return self
