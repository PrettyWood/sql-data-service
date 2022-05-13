from typing import TYPE_CHECKING, Any, TypeVar

from pypika import Field, Table
from pypika.dialects import PostgreSQLQuery
from pypika.enums import Comparator, Order
from pypika.terms import AnalyticFunction, BasicCriterion, Criterion

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, StepTable, get_aggregate_function

Self = TypeVar("Self", bound="PostgreSQLTranslator")

if TYPE_CHECKING:
    from pypika.dialects import PostgreSQLQueryBuilder
    from pypika.queries import QueryBuilder
    from weaverbird.pipeline.conditions import SimpleCondition
    from weaverbird.pipeline.steps import AggregateStep, TopStep, UniqueGroupsStep


class PostgreSQLMatching(Comparator):  # type: ignore[misc]
    similar_to = " SIMILAR TO "
    not_similar_to = " NOT SIMILAR TO "


class RowNumber(AnalyticFunction):  # type: ignore[misc]
    def __init__(self, **kwargs: Any) -> None:
        super().__init__("ROW_NUMBER", **kwargs)


class PostgreSQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.POSTGRESQL
    QUERY_CLS = PostgreSQLQuery

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

        if step.keep_original_granularity:
            current_query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)

            agg_query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
                *step.on, *agg_selected
            )
            agg_query = agg_query.groupby(*step.on)

            all_agg_col_names: list[str] = [x for agg in step.aggregations for x in agg.new_columns]

            query: "QueryBuilder" = self.QUERY_CLS.from_(current_query).select(*table.columns)
            query = query.select(
                *(Field(agg_col, table=agg_query) for agg_col in all_agg_col_names)
            )
            query = query.left_join(agg_query).on_field(*step.on)
            selected_col_names = [*table.columns, *all_agg_col_names]

        else:
            selected_cols: list[str | Field] = [*step.on, *agg_selected]
            selected_col_names: list[str] = [*step.on, *(f.alias for f in agg_selected)]
            query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*selected_cols)
            query = query.groupby(*step.on)
            for col in step.on:
                query = query.orderby(col, order=Order.asc)

        return query, StepTable(columns=selected_col_names)

    def _get_single_condition_criterion(
        self, condition: "SimpleCondition", table: StepTable
    ) -> Criterion:
        column_field: Field = Table(table.name)[condition.column]
        match condition.operator:
            case "matches":
                return BasicCriterion(
                    PostgreSQLMatching.similar_to,
                    column_field,
                    column_field.wrap_constant(_compliant_regex(condition.value)),
                )
            case "notmatches":
                return BasicCriterion(
                    PostgreSQLMatching.not_similar_to,
                    column_field,
                    column_field.wrap_constant(_compliant_regex(condition.value)),
                )
            case _:
                return super()._get_single_condition_criterion(condition, table)

    def top(self: Self, *, step: "TopStep", table: StepTable) -> tuple["QueryBuilder", StepTable]:
        if step.groups:
            sub_query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)

            the_table = Table(table.name)
            rank_on_field: Field = the_table[step.rank_on]
            groups_fields: list[Field] = [the_table[group] for group in step.groups]
            sub_query = sub_query.select(
                RowNumber()
                .over(*groups_fields)
                .orderby(rank_on_field, order=Order.desc if step.sort == "desc" else Order.asc)
            )
            query: "QueryBuilder" = self.QUERY_CLS.from_(sub_query).select(*table.columns)
            query = query.where(Field("row_number") == step.limit)
            return query, StepTable(columns=table.columns)

        return super().top(step=step, table=table)

    def uniquegroups(
        self: Self, *, step: "UniqueGroupsStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        query: "PostgreSQLQueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)
        return query.distinct_on(*step.on), StepTable(columns=table.columns)


SQLTranslator.register(PostgreSQLTranslator)


def _compliant_regex(pattern: str) -> str:
    """
    Like LIKE, the SIMILAR TO operator succeeds only if its pattern matches the entire string;
    this is unlike common regular expression behavior wherethe pattern
    can match any part of the string
    (see https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-SIMILARTO-REGEXP)
    """
    return f"%{pattern}%"
