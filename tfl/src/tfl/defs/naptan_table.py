import dagster as dg
import requests as rq
import duckdb as db
from dagster_duckdb import DuckDBResource
from concurrent.futures import ThreadPoolExecutor, as_completed


@dg.asset()
def naptan_table(db_conn: DuckDBResource) -> None:
    query_create_table = """
    CREATE or replace table station_naptan (
    naptan_id VARCHAR,
    station_name VARCHAR,
    entrance VARCHAR,
    PRIMARY KEY (naptan_id)
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
    
@dg.asset(
    deps=['naptan_table']    
)
def create_crowding_table(db_conn: DuckDBResource) -> None:
    query_to_create_table = """
    CREATE or replace table crowding_data (
        naptan_id VARCHAR,
        time_recorded_utc DATETIME,
        crowding_level FLOAT,
        PRIMARY KEY (naptan_id, time_recorded_utc)
    )
    """

    with db_conn.get_connection() as conn:
        conn.execute(query_to_create_table)
        conn.commit()

def request_crowding_data(station_naptan_in: str):
    crowding_api_url = f'https://api.tfl.gov.uk/crowding/{station_naptan_in}/Live'
    res = rq.get(crowding_api_url, timeout=10, verify=False).json()
    if res.get('dataAvailable'):
        return (res['timeUtc'], res['percentageOfBaseline'])
    return None

@dg.asset(
    deps=['naptan_table',
          'create_crowding_table']
)
def crowding_data(db_conn: DuckDBResource) -> None:
    naptan_ids = []
    with db_conn.get_connection() as conn:
        conn_res = conn.execute('select naptan_id from station_naptan').fetchall()
    
    for row in conn_res:
        station_naptan = row[0]
        naptan_ids.append(station_naptan)

    with ThreadPoolExecutor(max_workers=6) as executor:
        naptan_id_to_response = {}
        future_to_url = {executor.submit(request_crowding_data, naptan_id): naptan_id for naptan_id in naptan_ids}
        for future in as_completed(future_to_url):
            naptan_id = future_to_url[future]
            try:
                naptan_id_to_response[naptan_id] = future.result()
            except:
                print(f'Could not get response for naptan_id: {naptan_id}')

    data_to_insert = []
    for naptan_id in naptan_id_to_response.keys():
        naptan_id_crowding_data = naptan_id_to_response[naptan_id]
        if naptan_id_crowding_data is not None:
            data_to_insert.append((
                naptan_id,
                naptan_id_crowding_data[0],
                naptan_id_crowding_data[1]
            ))
    
    query_insert_into_crowding = """
    INSERT INTO crowding_data
        (naptan_id, time_recorded_utc, crowding_level)
    VALUES
        (?, ?, ?)
    """

    with db_conn.get_connection() as conn:
        conn.executemany(query_insert_into_crowding, data_to_insert)
        conn.commit()