from abc import ABC

# from typing_extensions import Self
from typing import Mapping, Sequence, TypeVar

from pypika import Query

from sql_data_service.dialects import SQLDialect

Self = TypeVar("Self", bound="BaseSQLTranslator")


class BaseSQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query

    def __init__(self: Self, *, tables_columns: Mapping[str, Sequence[str]] | None = None) -> None:
        self.tables_columns: Mapping[str, Sequence[str]] = tables_columns or {}
        self.query = self.QUERY_CLS()

    def domain(self: Self, *, table_name: str) -> Self:
        print(f"[{self.DIALECT}] Selecting all columns for table {table_name!r}")

        try:
            table_columns = self.tables_columns[table_name]
        except KeyError:
            raise KeyError(f"All columns are unknown for table {table_name!r}")
        else:
            self.query = self.query.from_(table_name).select(*table_columns)

        return self

    def get_query(self: Self) -> str:
        query_str: str = self.query.get_sql()
        return query_str
