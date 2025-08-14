import dagster as dg
import duckdb as db


@dg.definitions
def resources() -> dg.Definitions:
    conn = db.connect('../../../tfl_db.db')
    return dg.Definitions(resources={'db_conn':conn})
