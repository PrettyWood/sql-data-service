from typing import Mapping, Sequence

from weaverbird.pipeline import PipelineWithVariables

from sql_data_service.dialects import SQLDialect
from sql_data_service.translators import ALL_TRANSLATORS


def translate_pipeline(
    *,
    sql_dialect: SQLDialect,
    pipeline: PipelineWithVariables,
    tables_columns: Mapping[str, Sequence[str]],
    db_schema: str | None = None,
) -> str:
    translator_cls = ALL_TRANSLATORS[sql_dialect]
    translator = translator_cls(
        tables_columns=tables_columns,
        db_schema=db_schema,
    )
    return translator.get_query_str(steps=pipeline.steps)
