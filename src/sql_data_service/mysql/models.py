from typing import Literal

from pydantic import BaseModel


class MySQLConnectionConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3306
    user: str | None = None
    password: str | None = None
    database: str | None = None
    charset: str | None = None
    connect_timeout: int | None = None


class MySQLConnection(BaseModel):
    dialect: Literal["mysql"]
    config: MySQLConnectionConfig
