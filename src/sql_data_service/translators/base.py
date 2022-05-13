from abc import ABC
from dataclasses import dataclass, field

# from typing_extensions import Self
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, Sequence, TypeVar, cast

from pypika import Criterion, Field, JoinType, Order, Query, Schema, Table, functions

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


if TYPE_CHECKING:
    from typing import TypedDict

    from pypika.queries import QueryBuilder
    from weaverbird.pipeline import PipelineStep
    from weaverbird.pipeline.conditions import Condition, SimpleCondition
    from weaverbird.pipeline.steps import (
        DomainStep,
        FilterStep,
        LowercaseStep,
        RenameStep,
        SelectStep,
        SortStep,
        UppercaseStep,
    )

    class SingleFilterCondition(TypedDict):
        column: str
        operator: Literal[
            "eq",
            "ne",
            "gt",
            "ge",
            "lt",
            "le",
            "in",
            "nin",
            "matches",
            "notmatches",
            "isnull",
            "notnull",
        ]
        value: Any

    class OrFilterCondition(TypedDict):
        or_: list["FilterCondition"]  # type: ignore

    class AndFilterCondition(TypedDict):
        and_: list["FilterCondition"]  # type: ignore

    FilterCondition = SingleFilterCondition | OrFilterCondition | AndFilterCondition  # type: ignore

    class SortColumn(TypedDict):
        column: str
        order: Literal["asc", "desc"]

    AggregationFunction = Literal["sum"]

    class Aggregation(TypedDict):
        new_columns: list[str]
        agg_function: AggregationFunction
        columns: list[str]


@dataclass(kw_only=True)
class StepTable:
    columns: list[str]
    name: str | None = None


class SQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query

    def __init__(
        self: Self,
        *,
        tables_columns: Mapping[str, Sequence[str]] | None = None,
        db_schema: str | None = None,
    ) -> None:
        self._tables_columns: Mapping[str, Sequence[str]] = tables_columns or {}
        self._db_schema: Schema | None = Schema(db_schema) if db_schema is not None else None

    def __init_subclass__(cls) -> None:
        ALL_TRANSLATORS[cls.DIALECT] = cls

    def get_query(self: Self, *, steps: Sequence["PipelineStep"]) -> "QueryBuilder":
        step_queries: list["QueryBuilder"] = []
        step_tables: list[StepTable] = []

        for i, step in enumerate(steps):
            try:
                step_method: Callable[..., tuple["QueryBuilder", StepTable]] = getattr(
                    self, step.name
                )
            except AttributeError:
                raise NotImplementedError(f"[{self.DIALECT}] step {step.name} not yet implemented")

            if i == 0:
                assert step.name == "domain"
                step_query, step_table = step_method(step=step)
            else:
                step_query, step_table = step_method(step=step, table=step_tables[i - 1])

            step_queries.append(step_query)
            step_table.name = f"__step_{i}__"
            step_tables.append(step_table)

        query: "QueryBuilder" = self.QUERY_CLS
        for i, step_query in enumerate(step_queries):
            query = query.with_(step_query, step_tables[i].name)

        return query.from_(step_tables[-1].name).select("*")

    def get_query_str(self: Self, *, steps: Sequence["PipelineStep"]) -> str:
        query_str: str = self.get_query(steps=steps).get_sql()
        return query_str

    # All other methods implement step from https://weaverbird.toucantoco.com/docs/steps/,
    # the name of the method being the name of the step and the kwargs the rest of the params
    # def argmax(self: Self, *, column: str, groups: Sequence[str]) -> Self:
    #     return self.top(rank_on=column, limit=1, sort="desc", groups=groups)

    # def argmin(self: Self, *, column: str, groups: Sequence[str]) -> Self:
    #     return self.top(rank_on=column, limit=1, sort="asc", groups=groups)

    # def aggregate(
    #     self: Self,
    #     *,
    #     on: Sequence[str],
    #     aggregations: Sequence["Aggregation"],
    #     keep_original_granularity: bool = False,
    # ) -> Self:
    #     raise NotImplementedError(f"[{self.DIALECT}] aggregate is not implemented")

    def domain(
        self: Self,
        *,
        step: "DomainStep",
    ) -> tuple["QueryBuilder", StepTable]:
        try:
            selected_cols = self._tables_columns[step.domain]
        except KeyError:
            selected_cols = ["*"]

        query = self.QUERY_CLS.from_(step.domain).select(*selected_cols)
        return query, StepTable(columns=selected_cols)

    # def delete(self: Self, *, columns: Sequence[str]) -> Self:
    #     self._query_infos.selected = [
    #         col_field for col_field in self._query_infos.selected if col_field.name not in columns
    #     ]
    #     return self

    def _get_single_condition_criterion(
        self: Self, condition: "SimpleCondition", table: StepTable
    ) -> Criterion:
        column_field: Field = Table(table.name)[condition.column]

        match condition.operator:
            case "eq" | "ne" | "gt" | "ge" | "lt" | "le":
                import operator

                op = getattr(operator, condition.operator)
                return op(column_field, condition.value)
            case "in":
                return column_field.isin(condition.value)
            case "nin":
                return column_field.notin(condition.value)
            case "matches":
                return column_field.regexp(condition.value)
            case "notmatches":
                return column_field.regexp(condition.value).negate()
            case "isnull":
                return column_field.isnull()
            case "notnull":
                return column_field.isnotnull()
            case _:  # pragma: no cover
                raise KeyError(f"Operator {condition.operator!r} does not exist")

    def _get_filter_criterion(self: Self, condition: "Condition", table: StepTable) -> Criterion:
        from weaverbird.pipeline.conditions import (
            ConditionComboAnd,
            ConditionComboOr,
            SimpleCondition,
        )

        match condition.__class__.__name__:
            case "ConditionComboOr":
                assert isinstance(condition, ConditionComboOr)
                return Criterion.any(
                    (self._get_filter_criterion(condition, table) for condition in condition.or_)
                )
            case "ConditionComboAnd":
                assert isinstance(condition, ConditionComboAnd)
                return Criterion.all(
                    (self._get_filter_criterion(condition, table) for condition in condition.and_)
                )
            case _:
                assert isinstance(condition, SimpleCondition)
                return self._get_single_condition_criterion(condition, table)

    def filter(
        self: Self, *, step: "FilterStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        query: "QueryBuilder" = (
            self.QUERY_CLS.from_(table.name)
            .select(*table.columns)
            .where(self._get_filter_criterion(step.condition, table))
        )
        return query, StepTable(columns=table.columns)

    def lowercase(
        self: Self, *, step: "LowercaseStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_idx = table.columns.index(step.column)
        col_field: Field = Table(table.name)[step.column]

        new_cols = list(table.columns)
        new_cols[col_idx] = functions.Lower(col_field).as_(new_cols[col_idx])

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*new_cols)
        return query, StepTable(columns=table.columns)

    def rename(
        self: Self, *, step: "RenameStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        old_name, new_name = step.to_rename

        selected_col_fields: list[Field] = []

        for col_name in table.columns:
            if col_name == old_name:
                selected_col_fields.append(Field(name=col_name, alias=new_name))
            else:
                selected_col_fields.append(Field(name=col_name))

        query = self.QUERY_CLS.from_(table.name).select(*selected_col_fields)
        return query, StepTable(columns=[f.alias or f.name for f in selected_col_fields])

    def select(
        self: Self, *, step: "SelectStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        query = self.QUERY_CLS.from_(table.name).select(*step.columns)
        return query, StepTable(columns=step.columns)

    def sort(self: Self, *, step: "SortStep", table: StepTable) -> tuple["QueryBuilder", StepTable]:
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)

        for column_sort in step.columns:
            query = query.orderby(
                column_sort.column, order=Order.desc if column_sort.order == "desc" else Order.asc
            )

        return query, StepTable(columns=table.columns)

    # def _top_with_groups(
    #     self: Self, rank_on: str, limit: int, order: Order, groups: Sequence[str]
    # ) -> None:
    #     raise NotImplementedError(f"[{self.DIALECT}] top is not implemented with groups")

    # def top(
    #     self: Self,
    #     *,
    #     rank_on: str,
    #     limit: int,
    #     sort: Literal["asc", "desc"],
    #     groups: Sequence[str],
    # ) -> Self:
    #     order = Order.desc if sort == "desc" else Order.asc

    #     if groups:
    #         self._top_with_groups(rank_on, limit, order, groups)
    #     else:
    #         self._query_infos.orders[rank_on] = order
    #         self._query_infos.limit = limit

    #     return self

    # def uniquegroups(self: Self, *, on: Sequence[str]) -> Self:
    #     raise NotImplementedError(f"[{self.DIALECT}] uniquegroups is not implemented")

    def uppercase(
        self: Self, *, step: "UppercaseStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_idx = table.columns.index(step.column)
        col_field: Field = Table(table.name)[step.column]

        new_cols = list(table.columns)
        new_cols[col_idx] = functions.Upper(col_field).as_(new_cols[col_idx])

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*new_cols)
        return query, StepTable(columns=table.columns)


def get_aggregate_function(agg_function: "AggregationFunction") -> functions.AggregateFunction:
    match agg_function:
        case "sum":
            return functions.Sum
        case _:
            raise NotImplementedError(f"Aggregation for {agg_function!r} is not yet implemented")
