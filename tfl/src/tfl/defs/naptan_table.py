import dagster as dg


@dg.asset
def naptan_table(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
