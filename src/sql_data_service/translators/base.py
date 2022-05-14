from abc import ABC
from dataclasses import dataclass

# from typing_extensions import Self
from typing import TYPE_CHECKING, Callable, Literal, Mapping, Sequence, TypeVar

from pypika import Criterion, Field, Order, Query, Schema, Table, functions

from sql_data_service.dialects import SQLDialect

from . import ALL_TRANSLATORS

Self = TypeVar("Self", bound="SQLTranslator")


if TYPE_CHECKING:
    from pypika.queries import QueryBuilder
    from weaverbird.pipeline import PipelineStep
    from weaverbird.pipeline.conditions import Condition, SimpleCondition
    from weaverbird.pipeline.steps import (
        AggregateStep,
        ArgmaxStep,
        ArgminStep,
        ConvertStep,
        DeleteStep,
        DomainStep,
        FilterStep,
        FormulaStep,
        FromdateStep,
        LowercaseStep,
        RenameStep,
        SelectStep,
        SortStep,
        TextStep,
        TopStep,
        UniqueGroupsStep,
        UppercaseStep,
    )
    from weaverbird.pipeline.steps.aggregate import AggregateFn

    WeaverbirdCastType = Literal["integer", "float", "text", "date", "boolean"]


@dataclass(kw_only=True)
class StepTable:
    columns: list[str]
    name: str | None = None


class SQLTranslator(ABC):
    DIALECT: SQLDialect
    QUERY_CLS: Query
    DATA_TYPE_MAPPING: dict["WeaverbirdCastType", str] = {
        "integer": "INTEGER",
        "float": "DOUBLE",
        "text": "TEXT",
        "date": "DATE",
        "boolean": "BOOLEAN",
    }

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
    def _get_aggregate_function(
        self: Self, agg_function: "AggregateFn"
    ) -> functions.AggregateFunction:
        match agg_function:
            case "avg":
                return functions.Avg
            case "count":
                return functions.Count
            case "count distinct":
                return CountDistinct
            case "max":
                return functions.Max
            case "min":
                return functions.Min
            case "sum":
                return functions.Sum
            case _:  # pragma: no cover
                raise NotImplementedError(
                    f"[{self.DIALECT}] Aggregation for {agg_function!r} is not yet implemented"
                )

    def aggregate(
        self: Self, *, step: "AggregateStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        the_table = Table(table.name)
        agg_selected: list[Field] = []

        for aggregation in step.aggregations:
            agg_fn = self._get_aggregate_function(aggregation.agg_function)
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

    def argmax(
        self: Self, *, step: "ArgmaxStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        from weaverbird.pipeline.steps import TopStep

        return self.top(
            step=TopStep(rank_on=step.column, sort="desc", limit=1, groups=step.groups), table=table
        )

    def argmin(
        self: Self, *, step: "ArgminStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        from weaverbird.pipeline.steps import TopStep

        return self.top(
            step=TopStep(rank_on=step.column, sort="asc", limit=1, groups=step.groups), table=table
        )

    def convert(
        self: Self, *, step: "ConvertStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_fields: list[Field] = [Table(table.name)[col] for col in step.columns]
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
            *(c for c in table.columns if c not in step.columns),
            *(
                functions.Cast(col_field, self.DATA_TYPE_MAPPING[step.data_type]).as_(
                    col_field.name
                )
                for col_field in col_fields
            ),
        )
        return query, StepTable(columns=table.columns)

    def delete(
        self: Self, *, step: "DeleteStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        new_columns = [c for c in table.columns if c not in step.columns]
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*new_columns)
        return query, StepTable(columns=new_columns)

    def domain(
        self: Self,
        *,
        step: "DomainStep",
    ) -> tuple["QueryBuilder", StepTable]:
        try:
            selected_cols = list(self._tables_columns[step.domain])
        except KeyError:
            selected_cols = ["*"]

        query: "QueryBuilder" = self.QUERY_CLS.from_(
            Table(step.domain, schema=self._db_schema)
        ).select(*selected_cols)
        return query, StepTable(columns=selected_cols)

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
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)
        query = query.where(self._get_filter_criterion(step.condition, table))
        return query, StepTable(columns=table.columns)

    def formula(
        self: Self, *, step: "FormulaStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        from pypika.terms import LiteralValue

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)
        # TODO: support
        # - float casting with divisions
        # - whitespaces in column names
        # - strings
        # To do that we probably need to parse the formula with tokens
        # In the end we should be able to translate:
        #   [my age] + 1 / 2
        # into
        #   CAST("my age" AS float) + 1 / 2
        query = query.select(LiteralValue(step.formula).as_(step.new_column))
        return query, StepTable(columns=[*table.columns, step.new_column])

    def fromdate(
        self: Self, *, step: "FromdateStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_field: Field = Table(table.name)[step.column]
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(
            *(c for c in table.columns if c != step.column),
            functions.ToChar(col_field, step.format).as_(step.column),
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

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*selected_col_fields)
        return query, StepTable(columns=[f.alias or f.name for f in selected_col_fields])

    def select(
        self: Self, *, step: "SelectStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*step.columns)
        return query, StepTable(columns=step.columns)

    def sort(self: Self, *, step: "SortStep", table: StepTable) -> tuple["QueryBuilder", StepTable]:
        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)

        for column_sort in step.columns:
            query = query.orderby(
                column_sort.column, order=Order.desc if column_sort.order == "desc" else Order.asc
            )

        return query, StepTable(columns=table.columns)

    def text(self: Self, *, step: "TextStep", table: StepTable) -> tuple["QueryBuilder", StepTable]:
        from pypika.terms import ValueWrapper

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)
        query = query.select(ValueWrapper(step.text).as_(step.new_column))
        return query, StepTable(columns=[*table.columns, step.new_column])

    def top(self: Self, *, step: "TopStep", table: StepTable) -> tuple["QueryBuilder", StepTable]:
        if step.groups:
            raise NotImplementedError(f"[{self.DIALECT}] top is not implemented with groups")

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*table.columns)
        query = query.orderby(step.rank_on, order=Order.desc if step.sort == "desc" else Order.asc)
        query = query.limit(step.limit)
        return query, StepTable(columns=table.columns)

    def uniquegroups(
        self: Self, *, step: "UniqueGroupsStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        raise NotImplementedError(f"[{self.DIALECT}] uniquegroups is not implemented")

    def uppercase(
        self: Self, *, step: "UppercaseStep", table: StepTable
    ) -> tuple["QueryBuilder", StepTable]:
        col_idx = table.columns.index(step.column)
        col_field: Field = Table(table.name)[step.column]

        new_cols = list(table.columns)
        new_cols[col_idx] = functions.Upper(col_field).as_(new_cols[col_idx])

        query: "QueryBuilder" = self.QUERY_CLS.from_(table.name).select(*new_cols)
        return query, StepTable(columns=table.columns)


class CountDistinct(functions.Count):  # type: ignore[misc]
    def __init__(self, param: str | Field, alias: str | None = None) -> None:
        super().__init__(param, alias)
        self._distinct = True
