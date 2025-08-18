import dagster as dg
import os
from dagster_duckdb import DuckDBResource
import pandas as pd
import plotly.express as px


def create_images(naptan_ids: list[str], crowding_df: pd.DataFrame, station_df: pd.DataFrame) -> list[str]:
    # workdir_for_images = 'C:\\Users\\APatel1\\Documents\\dagster-capstone\\tfl_status\\tfl\\src\\tfl\\figures\\'
    workdir_for_images = os.path.dirname(os.path.realpath(__file__)) + '\\..\\figures\\'
    for naptan_id in naptan_ids:
        station_name = station_df['station_name'][station_df['naptan_id']==naptan_id].iloc[0]
        temp_crowding_data_df = crowding_df[crowding_df['naptan_id']==naptan_id].sort_values('time_recorded_utc')
        fig = px.line(temp_crowding_data_df, x='time_recorded_utc', y='crowding_level')
        fig.write_image(workdir_for_images + f'{station_name}_' + naptan_id + '_crowding.png', format='png')

@dg.asset(
    deps=['crowding_data']
)
def produce_crowding_data_figures(db_conn: DuckDBResource) -> None:
    query_station_id_name = """
    SELECT
        DISTINCT naptan_id,
        station_name,
        entrance
    FROM
        crowding_data
    JOIN
        station_naptan USING (naptan_id)
    """
    with db_conn.get_connection() as conn:
        naptan_id_to_name_df = conn.execute(query_station_id_name).df()

    naptan_ids = naptan_id_to_name_df['naptan_id'].to_list()
    query_get_crowding_data = """
    SELECT
        *
    FROM
        crowding_data
    """
    with db_conn.get_connection() as conn:
        crowding_data_df = conn.execute(query_get_crowding_data).df()

    create_images(naptan_ids, crowding_data_df, naptan_id_to_name_df)
    print('Images have been saved')
