from abc import ABC, abstractmethod
from typing import Any


class BaseSQLExecutor(ABC):
    @abstractmethod
    async def execute(self, sql_query: str) -> list[dict[str, Any]]:
        """Executes a SQL query"""

    @abstractmethod
    async def get_all_columns(self, table_name: str) -> list[str]:
        """Returns all columns of a table"""
