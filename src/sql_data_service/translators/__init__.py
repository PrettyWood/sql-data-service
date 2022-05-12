from typing import TYPE_CHECKING

from sql_data_service.dialects import SQLDialect

if TYPE_CHECKING:
    from .base import SQLTranslator

ALL_TRANSLATORS: dict[SQLDialect, type["SQLTranslator"]] = {}

from .athena import AthenaTranslator  # noqa
from .mysql import MySQLTranslator  # noqa
from .postgresql import PostgreSQLTranslator  # noqa
from .redshift import RedshiftQueryTranslator  # noqa
from .snowflake import SnowflakeTranslator  # noqa
