import aiomysql

from .models import MySQLConnectionConfig


async def get_connection(mysql_config: MySQLConnectionConfig) -> aiomysql.Connection:
    return await aiomysql.connect(
        host=mysql_config.host,
        port=mysql_config.port,
        user=mysql_config.user,
        password=mysql_config.password,
        db=mysql_config.database,
    )
