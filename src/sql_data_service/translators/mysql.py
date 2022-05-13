from typing import TYPE_CHECKING, TypeVar

from pypika import Field, Table
from pypika.dialects import MySQLQuery
from pypika.enums import Order

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, StepTable, get_aggregate_function

Self = TypeVar("Self", bound="SQLTranslator")


if TYPE_CHECKING:
    from pypika.queries import QueryBuilder
    from weaverbird.pipeline.steps import AggregateStep


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery

    def aggregate(
        self: Self, *, step: "AggregateStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        the_table = Table(table.name)
        agg_selected: list[Field] = []

        for aggregation in step.aggregations:
            agg_fn = get_aggregate_function(aggregation.agg_function)
            for i, column_name in enumerate(aggregation.columns):
                column_field: Field = the_table[column_name]
                new_agg_col = agg_fn(column_field).as_(aggregation.new_columns[i])
                agg_selected.append(new_agg_col)

        query: "QueryBuilder"
        selected_col_names: list[str]

        if step.keep_original_granularity:
            current_query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)

            agg_query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
                *step.on, *agg_selected
            )
            agg_query = agg_query.groupby(*step.on)

            all_agg_col_names: list[str] = [x for agg in step.aggregations for x in agg.new_columns]

            query = self.QUERY_CLS.from_(current_query).select(*table.columns)
            query = query.select(
                *(Field(agg_col, table=agg_query) for agg_col in all_agg_col_names)
            )
            query = query.left_join(agg_query).on_field(*step.on)
            selected_col_names = [*table.columns, *all_agg_col_names]

        else:
            selected_cols: list[str | Field] = [*step.on, *agg_selected]
            selected_col_names = [*step.on, *(f.alias for f in agg_selected)]
            query = self.QUERY_CLS.from_(table.name).select(*selected_cols)
            query = query.groupby(*step.on)
            for col in step.on:
                query = query.orderby(col, order=Order.asc)

        return query, StepTable(columns=selected_col_names)


SQLTranslator.register(MySQLTranslator)
