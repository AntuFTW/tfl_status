import dagster as dg
import duckdb as db
from dagster_duckdb import DuckDBResource

database_resource = DuckDBResource(
    database='../../../tfl_db.db'      # replaced with environment variable
)


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={'db_conn':database_resource})