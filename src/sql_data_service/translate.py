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
    translator = translator_cls(tables_columns=tables_columns, db_schema=db_schema)
    for step in pipeline.steps:
        try:
            translator_method = getattr(translator, step.name)
        except AttributeError:  # pragma: no cover
            raise NotImplementedError(
                f"step {step.name!r} is not yet implemented for {sql_dialect} translator"
            )
        else:
            translator = translator_method(**step.dict(exclude={"name"}))

    return translator.get_query()
