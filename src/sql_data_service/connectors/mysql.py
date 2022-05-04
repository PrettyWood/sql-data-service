# import aiomysql
# from typing import Any

# import aiomysql
# from weaverbird.pipeline import PipelineStepWithVariables

# from sql_data_service.models import DataRows, MySQLConnectionConfig


# async def get_connection(mysql_config: MySQLConnectionConfig) -> aiomysql.Connection:
#     return await aiomysql.connect(
#         host=mysql_config.host,
#         port=mysql_config.port,
#         user=mysql_config.user,
#         password=mysql_config.password,
#         db=mysql_config.database,
#     )


# async def execute(mysql_config: MySQLConnectionConfig, query: str) -> list[dict,]
# ) -> list[dict[str, Any]]:
#     conn = await get_connection(mysql_config)
#     sql_query = await translate_pipeline_mysql(pipeline, conn)

#     async with conn.cursor(aiomysql.DictCursor) as cur:
#         await cur.execute(sql_query)
#         dict_records: list[dict[str, Any]] = await cur.fetchall()

#     conn.close()
#     return dict_records
