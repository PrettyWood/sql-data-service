from enum import Enum


class SQLDialect(str, Enum):
    ATHENA = "athena"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
