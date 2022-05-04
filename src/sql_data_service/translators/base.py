from typing import Any, Callable

from typing_extensions import Self


class SQLTranslator:
    def __init__(self, table_columns: dict[str, list[str]] | None = None) -> None:
        self.table_columns: dict[str, list[str]] = table_columns or {}
        self.query_executor: Callable[[str], list[dict[str, Any]]] = None

    def domain(self, table_name: str) -> Self:
        if table_name not in self.table_columns:
            self.table_columns[table_name] = self.fetch_all_columns(table_name)
        return self

    async def fetch_all_columns(self, table_name: str) -> list[str]:
        get_columns_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        if self.query_executor is None:
            raise Exception(
                f"Need to set a query executor to retrieve all columns for table {table_name!r}"
            )

        # columns_records = [{'column_name': 'username'}, {'column_name': 'age'}, {'column_name': 'city'}]
        columns_records = self.query_executor(get_columns_query)
        return [r["column_name"] for r in columns_records]


################################ POSTGRES
# from typing import cast

# import asyncpg
# from pypika import Query
# from weaverbird.pipeline import PipelineWithVariables
# from weaverbird.pipeline.steps import DomainStep


# async def translate_pipeline_postgresql(
#     pipeline: PipelineWithVariables, conn: asyncpg.Connection
# ) -> str:
#     query = Query()
#     metadata = {}  # domain -> metadata on all columns

#     for step in pipeline.steps:
#         match step.name:
#             case "domain":
#                 assert isinstance(step, DomainStep)
#                 all_columns = await get_all_columns(step.domain, conn)
#                 metadata[step.domain] = {col_name: True for col_name in all_columns}
#                 query = query.from_(step.domain).select(*all_columns)
#             case _:  # pragma: no cover
#                 raise NotImplementedError("Step {step.name!r} is not yet implemented")

#     return cast(str, query.get_sql())


# async def get_all_columns(table: str, conn: asyncpg.Connection) -> list[str]:
#     get_columns_query = f"""
#         SELECT column_name
#         FROM information_schema.columns
#         WHERE table_name = '{table}'
#         ORDER BY ordinal_position
#     """
#     column_records = await conn.fetch(get_columns_query)
#     return [r[0] for r in column_records]


########################### MYSQL
# from typing import cast

# import aiomysql
# from pypika import MySQLQuery
# from weaverbird.pipeline import PipelineWithVariables
# from weaverbird.pipeline.steps import DomainStep


# async def translate_pipeline_mysql(
#     pipeline: PipelineWithVariables, conn: aiomysql.Connection
# ) -> str:
#     query = MySQLQuery()
#     metadata = {}  # domain -> metadata on all columns

#     for step in pipeline.steps:
#         match step.name:
#             case "domain":
#                 assert isinstance(step, DomainStep)
#                 all_columns = await get_all_columns(step.domain, conn)
#                 metadata[step.domain] = {col_name: True for col_name in all_columns}
#                 query = query.from_(step.domain).select(*all_columns)
#             case _:  # pragma: no cover
#                 raise NotImplementedError("Step {step.name!r} is not yet implemented")

#     return cast(str, query.get_sql())


# async def get_all_columns(table: str, conn: aiomysql.Connection) -> list[str]:
#     get_columns_query = f"""
#         SELECT column_name
#         FROM information_schema.columns
#         WHERE table_name = '{table}'
#         ORDER BY ordinal_position
#     """

#     cur = await conn.cursor()
#     await cur.execute(get_columns_query)
#     column_records = await cur.fetchall()
#     await cur.close()

#     return [r[0] for r in column_records]
