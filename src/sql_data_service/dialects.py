from enum import Enum


class SQLDialect(str, Enum):
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
