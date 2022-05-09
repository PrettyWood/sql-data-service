from abc import ABC
from dataclasses import dataclass

# from typing_extensions import Self
from typing import Mapping, Sequence, TypeVar

from pypika import Field, Query

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


@dataclass
class ColumnMetadata:
    name: str
    alias: str | None = None
    selected: bool = True


class SQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query

    def __init__(self: Self, *, tables_columns: Mapping[str, Sequence[str]] | None = None) -> None:
        self.main_table: str = ""
        self.tables_columns_metadata: dict[str, list[ColumnMetadata]] = {}
        if tables_columns:
            self.set_tables_columns(tables_columns)

    def __init_subclass__(cls) -> None:
        ALL_TRANSLATORS[cls.DIALECT] = cls

    def set_tables_columns(self: Self, tables_columns: Mapping[str, Sequence[str]]) -> None:
        for table_name, table_columns in tables_columns.items():
            self.tables_columns_metadata[table_name] = [
                ColumnMetadata(table_col) for table_col in table_columns
            ]

    def get_query(self: Self) -> str:
        try:
            selected_columns = tuple(
                Field(name=column.name, alias=column.alias)
                for column in self.tables_columns_metadata[self.main_table]
                if column.selected
            )
        except KeyError:
            selected_columns = ("*",)
        query_str: str = self.QUERY_CLS().from_(self.main_table).select(*selected_columns).get_sql()
        return query_str

    # All other methods implement step from https://weaverbird.toucantoco.com/docs/steps/,
    # the name of the method being the name of the step and the kwargs the rest of the params
    def domain(self: Self, *, domain: str) -> Self:
        self.main_table = domain
        return self

    def delete(self: Self, *, columns: Sequence[str]) -> Self:
        for col in self.tables_columns_metadata[self.main_table]:
            if col.name in columns:
                col.selected = False
        return self

    def rename(self: Self, *, to_rename: tuple[str, str]) -> Self:
        table_cols_metadata = self.tables_columns_metadata[self.main_table]
        col_metadata = [c for c in table_cols_metadata if to_rename[0]][0]
        col_metadata.alias = to_rename[1]
        return self

    def select(self: Self, *, columns: Sequence[str]) -> Self:
        if self.main_table not in self.tables_columns_metadata:
            self.set_tables_columns({self.main_table: columns})
        try:
            for col in self.tables_columns_metadata[self.main_table]:
                if col.name not in columns:
                    col.selected = False
        except KeyError:
            self.tables_columns_metadata[self.main_table] = []
        return self
