from abc import ABC

# from typing_extensions import Self
from typing import Mapping, Sequence, TypeVar

from pypika import Query

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


class SQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query

    def __init__(self: Self, *, tables_columns: Mapping[str, Sequence[str]] | None = None) -> None:
        self.tables_columns: Mapping[str, Sequence[str]] = tables_columns or {}
        self.query = self.QUERY_CLS()

    def __init_subclass__(cls) -> None:
        ALL_TRANSLATORS[cls.DIALECT] = cls

    def domain(self: Self, *, domain: str) -> Self:
        print(f"[{self.DIALECT}] Selecting all columns for table {domain!r}")

        try:
            table_columns = self.tables_columns[domain]
        except KeyError:
            raise KeyError(f"All columns are unknown for table {domain!r}")
        else:
            self.query = self.query.from_(domain).select(*table_columns)

        return self

    def get_query(self: Self) -> str:
        query_str: str = self.query.get_sql()
        return query_str
