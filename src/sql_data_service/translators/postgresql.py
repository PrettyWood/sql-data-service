from typing import TYPE_CHECKING, Any, TypeVar

from pypika import Field, Table
from pypika.dialects import PostgreSQLQuery
from pypika.enums import Comparator, Order
from pypika.terms import AnalyticFunction, BasicCriterion, Criterion

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, StepTable

Self = TypeVar("Self", bound="PostgreSQLTranslator")

if TYPE_CHECKING:
    from pypika.dialects import PostgreSQLQueryBuilder
    from pypika.queries import QueryBuilder
    from weaverbird.pipeline.conditions import SimpleCondition
    from weaverbird.pipeline.steps import TopStep, UniqueGroupsStep


class PostgreSQLMatching(Comparator):  # type: ignore[misc]
    similar_to = " SIMILAR TO "
    not_similar_to = " NOT SIMILAR TO "


class RowNumber(AnalyticFunction):  # type: ignore[misc]
    def __init__(self, **kwargs: Any) -> None:
        super().__init__("ROW_NUMBER", **kwargs)


class PostgreSQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.POSTGRESQL
    QUERY_CLS = PostgreSQLQuery

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
