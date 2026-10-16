# Contains the script to populate the raw tables, disruption tables and the flux table

# Packages imported
import logging
import json
import contextlib

from ingestion.ingestion_functions import BASE_URL, fetch_all_pages, parse_time, get_db_connection
import ingestion.ingestion_functions as igf

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

FLUX_COLUMNS = ("vehicle_journey_id", "station_id", "origin_id", "terminus_id",
                "base_time_in", "time_in", "base_time_out", "time_out",
                "line_id", "network_id", "physical_mode")
FLUX_NOT_NULL = ("vehicle_journey_id", "station_id", "origin_id", "terminus_id",
                 "line_id", "network_id", "physical_mode")
FLUX_INSERT_REQUEST = """INSERT INTO flux (vehicle_journey_id, station_id, origin_id, terminus_id, base_time_in, time_in, base_time_out, time_out, line_id, network_id, physical_mode) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                         ON CONFLICT (vehicle_journey_id, station_id) 
                         DO UPDATE SET 
                           origin_id      = EXCLUDED.origin_id, 
                           terminus_id    = EXCLUDED.terminus_id, 
                           base_time_in   = EXCLUDED.base_time_in, 
                           time_in        = EXCLUDED.time_in, 
                           base_time_out  = EXCLUDED.base_time_out, 
                           time_out       = EXCLUDED.time_out, 
                           line_id        = EXCLUDED.line_id, 
                           network_id     = EXCLUDED.network_id, 
                           physical_mode  = EXCLUDED.physical_mode,
                           updated_at     = now()
                         WHERE (flux.origin_id, flux.terminus_id, flux.base_time_in, flux.time_in, flux.base_time_out, 
                                flux.time_out, flux.line_id, flux.network_id, flux.physical_mode)
                         IS DISTINCT FROM (EXCLUDED.origin_id, EXCLUDED.terminus_id, EXCLUDED.base_time_in, 
                                           EXCLUDED.time_in, EXCLUDED.base_time_out, EXCLUDED.time_out, 
                                           EXCLUDED.line_id, EXCLUDED.network_id, EXCLUDED.physical_mode)
                      """
def run_ingest_disruptions():
    """
    Called by Airflow, it opens the connection and run ingest_disruptions
    :return: Nothing
    """
    init_n_req = igf.request_count
    with contextlib.closing(get_db_connection()) as conn:
        ingest_disruptions(conn)
    logger.info(f"Number of requests done for one execution of the disruptions ingestion : {igf.request_count - init_n_req}")
    return igf.request_count - init_n_req

def ingest_disruptions(conn):
    """
    Takes a connection and saves in the database the information about disruption tables
    :param conn: the psycopg2 connector to execute queries
    :return: Nothing
    """
    # raw disruptions
    disrupt_data = fetch_all_pages(BASE_URL, "disruptions", params=None)
    # disruptions
    disruption_rows = [(row.get("disruption_id"), row.get("status"), row.get("severity", {}).get("name"), row.get("message")) for row in disrupt_data["disruptions"]]
    # disrupted_stations
    disr_stations_data = []
    for row in disrupt_data["disruptions"]:
        if row.get("impacted_objects") is not None:
            for obj in row.get("impacted_objects"):
                if obj.get('impacted_stops') is not None:
                    for imp_stop in obj.get('impacted_stops'):
                        if imp_stop.get("stop_point", {}).get("id") is None:
                            raise ValueError("A disrupted station_id cannot be None")
                        else:
                            disr_stations_data.append(
                                (row.get("disruption_id"), imp_stop.get("stop_point", {}).get("id")))
    # disrupted_objects
    disr_object_data = []
    for row in disrupt_data["disruptions"]:
        if row.get("impacted_objects") is not None:
            for obj in row.get("impacted_objects"):
                if obj.get("pt_object", {}).get("id") is None:
                    raise ValueError("A disrupted object_id cannot be None")
                else:
                    if obj.get("pt_object", {}).get("embedded_type") == "trip":
                        disr_obj_type = obj.get("pt_object", {}).get("id").split(":")[-1]
                    else:
                        disr_obj_type = None
                    disr_object_data.append((row.get("disruption_id"), obj.get("pt_object", {}).get("id"),
                                             obj.get("pt_object", {}).get("embedded_type"), disr_obj_type))
    # disruption_periods
    disr_period_data = []
    for row in disrupt_data["disruptions"]:
        if row.get("application_periods") is not None:
            for period in row.get("application_periods"):
                disr_period_data.append(
                    (row.get("disruption_id"), parse_time(period.get("begin")), parse_time(period.get("end"))))

    commands = ["INSERT INTO disruptions (id, status, severity, message) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, severity = EXCLUDED.severity, message = EXCLUDED.message",
                "INSERT INTO disrupted_stations (disruption_id, station_id) VALUES (%s, %s) ON CONFLICT (disruption_id, station_id) DO NOTHING",
                "INSERT INTO disrupted_objects (disruption_id, object_id, embedded_type, physical_mode) VALUES (%s, %s, %s, %s) ON CONFLICT (disruption_id, object_id) DO NOTHING",
                "INSERT INTO disruption_periods (disruption_id, period_start, period_end) VALUES (%s, %s, %s) ON CONFLICT (disruption_id, (COALESCE(period_start, '-infinity'::timestamptz)), (COALESCE(period_end, 'infinity'::timestamptz))) DO NOTHING"]
    tables = ["disruptions", "disrupted_stations", "disrupted_objects", "disruption_periods"]
    data_list = [disruption_rows, disr_stations_data, disr_object_data, disr_period_data]

    with conn.cursor() as cur:
        failed = []
        # Filling the raw disruption table
        try:
            cur.execute("INSERT INTO raw_disruptions (raw_json) VALUES (%s)", (json.dumps(disrupt_data),))
            conn.commit()
        except Exception as e:
            logger.exception(f"Disruptions ingestion failed for raw table: {e}")
            conn.rollback()
            failed.append("raw_disruptions")
        # Filling the disruption tables
        for table_ite, table in enumerate(tables):
            try:
                cur.executemany(commands[table_ite], data_list[table_ite])
                conn.commit()
            except Exception:
                logger.exception("Disruptions ingestion failed for table %s", table)
                conn.rollback()
                failed.append(table)
        if failed:
            raise RuntimeError(f"Ingestion failed for the tables {failed}")

def parse_flux_data(raw_flux_row):
    """
    Parses a raw_flux_row variable containing the data from an arrival or a departure obtained with a request to obtain
    data to save in the database
    :param raw_flux_row: dict containing requested data from an arrival or a departure row
    :return: flux data row to be saved into the database
    """
    links = {l.get("type"): l.get("id") for l in raw_flux_row.get("links", [])}
    sdt = raw_flux_row.get("stop_date_time", {})
    sdt_links = {l.get("rel") : l.get("id") for l in sdt.get("links", [])}
    return {
        "vehicle_journey_id": links.get("vehicle_journey"),
        "station_id": (raw_flux_row.get("stop_point") or {}).get("id"),
        "origin_id": sdt_links.get("origins"),
        "terminus_id": sdt_links.get("terminus"),
        "base_time_in": parse_time(sdt.get("base_arrival_date_time")),
        "time_in": parse_time(sdt.get("arrival_date_time")),
        "base_time_out": parse_time(sdt.get("base_departure_date_time")),
        "time_out": parse_time(sdt.get("departure_date_time")),
        "line_id": links.get("line"),
        "network_id": links.get("network"),
        "physical_mode": links.get("physical_mode"),
    }

def build_flux_rows(dep_data, arr_data, known_ids=None):
    """
    Takes arrivals and departures data and combines them into flux data without duplicates.
    Handles problematic rows that would prevent the whole dataset from being saved into the database
    and displays the problematic row.
    :param dep_data:
    :param arr_data:
    :return:
    """
    deduplicated_data = {}
    stats = {"incomplete": 0, "unknown_ref": 0, "unknown_ids": set()}
    for raw_row in dep_data.get("departures", []) + arr_data.get("arrivals", []):
        row = parse_flux_data(raw_row)
        if any(row.get(col) is None for col in FLUX_NOT_NULL):
            stats["incomplete"] += 1
            continue
        if known_ids is not None:
            refs = [(row["station_id"], "stations"), (row["origin_id"], "areas"),
                    (row["terminus_id"], "areas"), (row["line_id"], "lines"),
                    (row["network_id"], "networks")]
            missing = [id for id, kind in refs if id not in known_ids[kind]]
            if missing:
                stats["unknown_ref"] += 1
                stats["unknown_ids"].update(missing)
                continue
        deduplicated_data[(row["vehicle_journey_id"], row["station_id"])] = tuple(row[col] for col in FLUX_COLUMNS)
    return list(deduplicated_data.values()), stats

def get_known_ids(conn):
    queries = {"stations": "SELECT station_id FROM stations",
               "areas": "SELECT area_id from stop_areas",
               "lines": "SELECT line_id from lines",
               "networks": "SELECT network_id from networks"}
    known_ids = {}
    with conn.cursor() as cur:
        for kind, query in queries.items():
            cur.execute(query)
            known_ids[kind] = {req[0] for req in cur.fetchall()}
    return known_ids

def ingest_one_area_flux(conn, area_id, known_ids=None):
    dep_data = fetch_all_pages(BASE_URL, f"stop_areas/{area_id}/departures", params=None)
    arr_data = fetch_all_pages(BASE_URL, f"stop_areas/{area_id}/arrivals", params=None)
    with conn.cursor() as cur:
        # raw departures
        cur.execute("INSERT INTO raw_departures (station_id, raw_json) VALUES (%s, %s)", (area_id, json.dumps(dep_data)))
        # raw arrivals
        cur.execute("INSERT INTO raw_arrivals (station_id, raw_json) VALUES (%s, %s)", (area_id, json.dumps(arr_data)))
    conn.commit()
    flux_data, stats = build_flux_rows(dep_data, arr_data, known_ids)
    with conn.cursor() as cur:
        cur.executemany(FLUX_INSERT_REQUEST, flux_data)
    conn.commit()
    return len(flux_data), stats

def ingest_flux(conn, area_ids):
    """
    Takes a connection and saves in the database the information about permanent tables
    :param conn: the psycopg2 connector to execute queries
    :param area_ids: the areas for which we want to ingest the flux data
    :return: The number of stations considered
    """
    known_ids = get_known_ids(conn)
    failed, total = [], {"rows": 0, "incomplete": 0, "unknown_ref": 0}
    for area_id in area_ids:
        try:
            n_row, stats = ingest_one_area_flux(conn, area_id, known_ids=known_ids)
            total["rows"] += n_row
            total["incomplete"] += stats["incomplete"]
            total["unknown_ref"] += stats["unknown_ref"]
            if stats["incomplete"] or stats["unknown_ref"]:
                logger.warning(f"{area_id} : {stats['incomplete']} incomplete lines, {stats['unknown_ref']} unknown ids : {sorted(stats['unknown_ids'])}")
        except Exception as e:
            logger.exception(f"Ingestion failed for {area_id}: {e}")
            conn.rollback()
            failed.append(area_id)
    logger.info(f"Flux ingestion : {total}")
    if failed:
        raise RuntimeError(f"{len(failed)}/{len(area_ids)} stations failed : {failed}")
    return {"n_stations": len(area_ids), **total}

def run_ingest_flux():
    """
    Called by Airflow, it opens the connection and run ingest_flux
    :return: ingest_flux value
    """
    init_n_req = igf.request_count
    with contextlib.closing(get_db_connection()) as conn:
        considered_areas = ['Tours', 'Saint-Pierre-des-Corps', 'Paris - Gare de Lyon - Hall 1 & 2',
                            'Paris - Montparnasse - Hall 1 & 2', 'Paris Montparnasse Hall 3 Vaugirard',
                            'Paris Austerlitz', "Paris Bercy Bourgogne - Pays d'Auvergne", 'Paris Est', 'Paris Nord',
                            'Paris Saint-Lazare', 'Nevers', 'Rennes', 'Toulouse Matabiau', 'Montpellier Saint-Roch',
                            'Montpellier Sud de France', 'Lyon Part Dieu', 'Lyon Perrache', 'Lyon Saint-Exupéry TGV',
                            'Bordeaux Saint-Jean', 'Strasbourg', 'Lille Flandres', 'Lille Europe',
                            'Marseille Saint-Charles',
                            'Marseille Blancarde', 'Dijon', 'Orléans', 'Nantes', 'Rouen Rive Droite']
        area_ids = []
        with conn.cursor() as cur:
            for area in considered_areas:
                cur.execute("SELECT area_id FROM stop_areas WHERE name = %s", (area,))
                result = cur.fetchall()
                if len(result) == 0:
                    logger.warning(f"Station {area} not found in the stop_areas table")
                else:
                    area_ids.append(result[0][0])
                    logger.debug(f"{area} : {result[0][0]}")
        ret_ingestion = ingest_flux(conn, area_ids)
        ret_ingestion["n_request"] = igf.request_count - init_n_req
        logger.info(f"Number of requests done for one execution of the flux ingestion : {ret_ingestion['n_request']}")
        return ret_ingestion
