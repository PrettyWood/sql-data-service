from abc import ABC, abstractmethod

from sql_data_service.models import DataRows


class BaseSQLExecutor(ABC):
    @abstractmethod
    async def execute(self, sql_query: str) -> DataRows:
        """Executes a SQL query"""
