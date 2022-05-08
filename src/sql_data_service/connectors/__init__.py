from typing import TYPE_CHECKING

from sql_data_service.dialects import SQLDialect

if TYPE_CHECKING:
    from .base import SQLExecutor

ALL_EXECUTORS: dict[SQLDialect, type["SQLExecutor"]] = {}

from .mysql import MySQLExecutor  # noqa
from .postgresql import PostgreSQLExecutor  # noqa
