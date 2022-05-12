from typing import TYPE_CHECKING, Sequence, TypeVar

from pypika import Field
from pypika.dialects import MySQLQuery
from pypika.enums import Order

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator, get_aggregate_function

Self = TypeVar("Self", bound="SQLTranslator")


if TYPE_CHECKING:
    from .base import Aggregation


class MySQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.MYSQL
    QUERY_CLS = MySQLQuery

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
            raise NotImplementedError(
                "[mysq] aggregate not yet implemented with keepOriginalGranularity"
            )
        else:
            self._query_infos.selected = [*on, *agg_selected]
            self._query_infos.groupbys = list(on)
            self._query_infos.orders = {k: Order.asc for k in on}

        return self


SQLTranslator.register(MySQLTranslator)
