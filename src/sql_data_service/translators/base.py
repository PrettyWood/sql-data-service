# from typing_extensions import Self
from typing import Mapping, Sequence, TypeVar

from pypika import Query

Self = TypeVar("Self", bound="SQLTranslator")


class SQLTranslator:
    def __init__(self: Self, *, tables_columns: Mapping[str, Sequence[str]] | None = None) -> None:
        self.tables_columns: Mapping[str, Sequence[str]] = tables_columns or {}
        self.query = Query()

    def domain(self: Self, *, table_name: str) -> Self:
        print(f"Selecting all columns for table {table_name!r}")

        try:
            table_columns = self.tables_columns[table_name]
        except KeyError:
            raise KeyError(f"All columns are unknown for table {table_name!r}")
        else:
            self.query = self.query.from_(table_name).select(*table_columns)

        return self

    def get_query(self: Self) -> str:
        return self.query.get_sql()


sql_translator = SQLTranslator(tables_columns={"users": ["username", "age", "city"]})
assert (
    sql_translator.domain(table_name="users").get_query()
    == 'SELECT "username","age","city" FROM "users"'
)


# async def fetch_all_columns(self, table_name: str) -> list[str]:
#     get_columns_query = f"""
#         SELECT column_name
#         FROM information_schema.columns
#         WHERE table_name = '{table_name}'
#         ORDER BY ordinal_position
#     """
#     if self.query_executor is None:
#         raise Exception(
#             f"Need to set a query executor to retrieve all columns for table {table_name!r}"
#         )

#     # records = [{'column_name': 'username'}, {'column_name': 'age'}, {'column_name': 'city'}]
#     columns_records = self.query_executor(get_columns_query)
#     return [r["column_name"] for r in columns_records]


# ############################### POSTGRES
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


# ########################## MYSQL
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
