import dagster as dg
import requests as rq
import duckdb as db


@dg.asset
def naptan_table(context: dg.AssetExecutionContext) -> dg.MaterializeResult:

