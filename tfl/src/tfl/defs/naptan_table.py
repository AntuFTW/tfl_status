import dagster as dg
import requests as rq
import duckdb as db
from dagster_duckdb import DuckDBResource
import json


@dg.asset()
def naptan_table(db_conn: DuckDBResource) -> None:
    query_create_table = """
    CREATE TABLE IF NOT EXISTS station_naptan (
    naptan_id VARCHAR,
    station_name VARCHAR,
    entrance VARCHAR
    )
    """

    station_list = []

    res = rq.get('https://api.tfl.gov.uk/StopPoint/Mode/tube', timeout=30, verify=False).json()
    all_stop_points = res['stopPoints']
    for stop_point in all_stop_points:
        station_list.append((
            stop_point['naptanId'],
            stop_point['commonName'],
            stop_point.get('indicator')
        ))

    query_insert_to_naptan = f"""
    INSERT INTO station_naptan
    (naptan_id, station_name, entrance)
    VALUES
    (?, ?, ?)
    """

    with db_conn.get_connection() as conn:
        conn.execute(query_create_table)
        conn.executemany(query_insert_to_naptan, station_list)
        conn.commit()
    
@dg.asset()
def crowding_table(db_conn: DuckDBResource) -> None:
    pass

