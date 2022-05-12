from pypika.dialects import Query

from sql_data_service.dialects import SQLDialect

from .base import SQLTranslator


class AthenaTranslator(SQLTranslator):
    DIALECT = SQLDialect.ATHENA
    QUERY_CLS = Query


SQLTranslator.register(AthenaTranslator)
