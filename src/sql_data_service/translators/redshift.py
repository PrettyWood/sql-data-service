from typing import TYPE_CHECKING, Sequence, TypeVar

from pypika import Field, JoinType, Order, Table
from pypika.dialects import RedshiftQuery

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, get_aggregate_function

Self = TypeVar("Self", bound="RedshiftQueryTranslator")

if TYPE_CHECKING:
    from .base import Aggregation


class RedshiftQueryTranslator(SQLTranslator):
    DIALECT = SQLDialect.REDSHIFT
    QUERY_CLS = RedshiftQuery

    def aggregate(
        self: Self,
        *,
        on: Sequence[str],
        aggregations: Sequence["Aggregation"],
        keep_original_granularity: bool = False,
    ) -> Self:

        agg_selected: list[Field] = []
        for aggregation in aggregations:
            agg_fn = get_aggregate_function(aggregation["agg_function"])
            for i, column in enumerate(aggregation["columns"]):
                column_field: Field = getattr(self._query_infos.from_, column)
                new_agg_col = agg_fn(column_field).as_(aggregation["new_columns"][i])
                agg_selected.append(new_agg_col)

        if keep_original_granularity:
            self._query_infos.sub_queries["__original__"] = self.get_query()
            self._query_infos.sub_queries["__aggregated__"] = (
                self.QUERY_CLS.from_(self._query_infos.from_)
                .select(*on, *agg_selected)
                .groupby(*on)
            )

            left_table = Table("__original__")
            right_table = Table("__aggregated__")
            all_agg_col_names = [x for agg in aggregations for x in agg["new_columns"]]
            self._query_infos.from_ = left_table
            self._query_infos.selected = [
                *(Field(name=f.name, table=left_table) for f in self._query_infos.selected),
                *(Field(name=col_name, table=right_table) for col_name in all_agg_col_names),
            ]
            self._query_infos.joins.append((right_table, JoinType.left, tuple(on)))
        else:
            self._query_infos.selected = [*on, *agg_selected]
            self._query_infos.groupbys = list(on)
            self._query_infos.orders = {k: Order.asc for k in on}

        return self


SQLTranslator.register(RedshiftQueryTranslator)
