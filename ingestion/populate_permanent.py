# Contains the script to fill the stop_areas, stations, lines and networks tables

# Packages imported
from ingestion.ingestion_functions import BASE_URL, fetch_all_pages, get_db_connection
import ingestion.ingestion_functions as igf
import contextlib
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def ingest_permanent_tables(conn):
    """
    Takes a connection and saves in the database the information about permanent tables
    :param conn: the psycopg2 connector to execute queries
    :return: Nothing
    """
    with conn.cursor() as cur:
        commands = ["INSERT INTO stop_areas (area_id, name, lon, lat) VALUES (%s, %s, %s, %s) ON CONFLICT (area_id) DO UPDATE SET name=excluded.name, lon=excluded.lon, lat=excluded.lat",
                    "INSERT INTO stations (station_id, station_name, area_id, lon, lat) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (station_id) DO UPDATE SET station_name=excluded.station_name, area_id=excluded.area_id, lon=excluded.lon, lat=excluded.lat",
                    "INSERT INTO lines (line_id, line_name) VALUES (%s, %s) ON CONFLICT (line_id) DO UPDATE SET line_name=excluded.line_name",
                    "INSERT INTO networks (network_id, network_name) VALUES (%s, %s) ON CONFLICT (network_id) DO UPDATE SET network_name=excluded.network_name"]
        req_keyword = ["stop_areas", "stop_points", "lines", "networks"]
        failed = []
        for key_ite, keyword in enumerate(req_keyword):
            try:
                cur.executemany(commands[key_ite], fetch_all_pages(BASE_URL, keyword))
                conn.commit()
            except Exception:
                logger.exception("Ingestion failed for %s", keyword)
                conn.rollback()
                failed.append(keyword)
        if failed:
            raise RuntimeError(f"Ingestion failed for the keywords {failed}")

def run_ingest_permanent_tables():
    """
    Called by Airflow, it opens the connection and run ingest_permanent_tables
    :return: Nothing
    """
    init_n_req = igf.request_count
    with contextlib.closing(get_db_connection()) as conn:
        ingest_permanent_tables(conn)
    logger.info(f"Number of requests done for one execution of the permanent tables ingestion : {igf.request_count - init_n_req}")
    return igf.request_count - init_n_req

