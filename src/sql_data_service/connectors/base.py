from abc import ABC, abstractmethod
from typing import Any

from sql_data_service.dialects import SQLDialect

from . import ALL_EXECUTORS


class SQLExecutor(ABC):
    DIALECT: SQLDialect

    def __init__(self, conn_config: Any) -> None:
        self.conn_config = conn_config

    def __init_subclass__(cls) -> None:
        ALL_EXECUTORS[cls.DIALECT] = cls

    @abstractmethod
    async def execute(self, sql_query: str) -> list[dict[str, Any]]:
        """Executes a SQL query"""

    @abstractmethod
    async def get_all_columns(self, table_name: str) -> list[str]:
        """Returns all columns of a table"""
