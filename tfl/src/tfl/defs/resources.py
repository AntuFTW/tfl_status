import dagster as dg
import duckdb as db
from dagster_duckdb import DuckDBResource
import os

workdir_for_this_file = os.path.dirname(os.path.realpath(__file__))
database_resource = DuckDBResource(
    database=workdir_for_this_file + '\\..\\..\\..\\tfl_db.db'      # replaced with environment variable
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={'db_conn':database_resource})