import dagster as dg

update_crowding_data_asset_selection = dg.AssetSelection.assets([
    'crowding_data'
])

update_crowding_data = dg.define_asset_job(
    name = 'update_crowding_data',
    selection = update_crowding_data_asset_selection
)
