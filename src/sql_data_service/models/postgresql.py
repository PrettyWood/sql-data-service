from pydantic import BaseModel


class PostgreSQLConnectionConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5432
    user: str | None = None
    password: str | None = None
    database: str | None = None
    charset: str | None = None
    connect_timeout: int | None = None
