from typing import TYPE_CHECKING

from sql_data_service.dialects import SQLDialect

if TYPE_CHECKING:
    from .base import SQLTranslator

ALL_TRANSLATORS: dict[SQLDialect, type["SQLTranslator"]] = {}

from .mysql import MySQLTranslator  # noqa
from .postgresql import PostgreSQLTranslator  # noqa
