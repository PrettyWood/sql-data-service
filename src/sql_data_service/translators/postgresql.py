from typing import TYPE_CHECKING, TypeVar

from pypika import Field, PostgreSQLQuery
from pypika.enums import Comparator
from pypika.terms import BasicCriterion, Criterion

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator

Self = TypeVar("Self", bound="PostgreSQLTranslator")

if TYPE_CHECKING:
    from .base import SingleFilterCondition


class PostgreSQLMatching(Comparator):  # type: ignore[misc]
    similar_to = " SIMILAR TO "
    not_similar_to = " NOT SIMILAR TO "


def _compliant_regex(pattern: str) -> str:
    """
    Like LIKE, the SIMILAR TO operator succeeds only if its pattern matches the entire string;
    this is unlike common regular expression behavior wherethe pattern
    can match any part of the string
    (see https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-SIMILARTO-REGEXP)
    """
    return f"%{pattern}%"


class PostgreSQLTranslator(SQLTranslator):
    DIALECT = SQLDialect.POSTGRESQL
    QUERY_CLS = PostgreSQLQuery

    def _get_single_condition_criterion(self, condition: "SingleFilterCondition") -> Criterion:
        column_field: Field = getattr(self._query_infos.from_, condition["column"])
        match condition["operator"]:
            case "matches":
                return BasicCriterion(
                    PostgreSQLMatching.similar_to,
                    column_field,
                    column_field.wrap_constant(_compliant_regex(condition["value"])),
                )
            case "notmatches":
                return BasicCriterion(
                    PostgreSQLMatching.not_similar_to,
                    column_field,
                    column_field.wrap_constant(_compliant_regex(condition["value"])),
                )
            case _:
                return super()._get_single_condition_criterion(condition)


SQLTranslator.register(PostgreSQLTranslator)
