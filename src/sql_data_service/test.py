from pypika import PostgreSQLQuery

# step select
selected = ["A", "B", "C"]
from_ = "table"

# step delete "C"
selected = ["A", "B"]

query = PostgreSQLQuery.from_(from_).select(*selected)
print(query.get_sql())
