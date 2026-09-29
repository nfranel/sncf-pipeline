# Contains the script to populate the raw tables, disruption tables and the flux table

# Packages imported
import logging
import os
import psycopg2
import json

from ingestion_functions import BASE_URL, fetch_all_pages, parse_time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

considered_areas = ['Tours', 'Saint-Pierre-des-Corps', 'Paris - Gare de Lyon - Hall 1 & 2',
                    'Paris - Montparnasse - Hall 1 & 2', 'Paris Montparnasse Hall 3 Vaugirard',
                    'Paris Austerlitz', "Paris Bercy Bourgogne - Pays d'Auvergne", 'Paris Est', 'Paris Nord',
                    'Paris Saint-Lazare', 'Nevers', 'Rennes', 'Toulouse Matabiau', 'Montpellier Saint-Roch',
                    'Montpellier Sud de France', 'Lyon Part Dieu', 'Lyon Perrache', 'Lyon Saint-Exupéry TGV',
                    'Bordeaux Saint-Jean', 'Strasbourg', 'Lille Flandres', 'Lille Europe', 'Marseille Saint-Charles',
                    'Marseille Blancarde', 'Dijon', 'Orléans', 'Nantes', 'Rouen Rive Droite']

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
)
cur = conn.cursor()

try:
    area_ids = []
    for area in considered_areas:
        cur.execute("SELECT area_id FROM stop_areas WHERE name = %s", (area,))
        result = cur.fetchone()
        if result is None:
            logger.warning(f"Station {area} not found in the stop_areas table")
        else:
            area_ids.append(result[0])
            logger.debug(f"{area} : {result[0]}")

except Exception as e:
    logger.error(f"Fetching station id failed : {e}")
    conn.rollback()
    raise

disrupt_data = fetch_all_pages(BASE_URL, "disruptions", params=None)

try:
    # raw disruptions
    cur.execute("INSERT INTO raw_disruptions (raw_json) VALUES (%s)", (json.dumps(disrupt_data),))

    # disruptions
    cur.executemany("INSERT INTO disruptions (id, status, severity, message) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, severity = EXCLUDED.severity, message = EXCLUDED.message",
                    [(row.get("disruption_id"), row.get("status"), row.get("severity", {}).get("name"), row.get("message")) for
                     row in disrupt_data["disruptions"]])

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
                            disr_stations_data.append((row.get("disruption_id"), imp_stop.get("stop_point", {}).get("id")))
    cur.executemany("INSERT INTO disrupted_stations (disruption_id, station_id) VALUES (%s, %s) ON CONFLICT (disruption_id, station_id) DO NOTHING",
        disr_stations_data)

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
    cur.executemany("INSERT INTO disrupted_objects (disruption_id, object_id, embedded_type, physical_mode) VALUES (%s, %s, %s, %s) ON CONFLICT (disruption_id, object_id) DO NOTHING",
                    disr_object_data)

    # disruption_periods
    disr_period_data = []
    for row in disrupt_data["disruptions"]:
        if row.get("application_periods") is not None:
            for period in row.get("application_periods"):
                disr_period_data.append((row.get("disruption_id"), parse_time(period.get("begin")), parse_time(period.get("end"))))
    cur.executemany("INSERT INTO disruption_periods (disruption_id, period_start, period_end) VALUES (%s, %s, %s) ON CONFLICT (disruption_id, (COALESCE(period_start, '-infinity'::timestamptz)), (COALESCE(period_end, 'infinity'::timestamptz))) DO NOTHING",
                    disr_period_data)

    conn.commit()
except Exception as e:
    logger.error(f"Disruptions ingestion failed: {e}")
    conn.rollback()

for area_id in area_ids:
    try:
        dep_data = fetch_all_pages(BASE_URL, f"stop_areas/{area_id}/departures", params=None)
        arr_data = fetch_all_pages(BASE_URL, f"stop_areas/{area_id}/arrivals", params=None)

        # raw departures
        cur.execute("INSERT INTO raw_departures (station_id, raw_json) VALUES (%s, %s)", (area_id, json.dumps(dep_data)))

        # raw arrivals
        cur.execute("INSERT INTO raw_arrivals (station_id, raw_json) VALUES (%s, %s)", (area_id, json.dumps(arr_data)))

        # flux
        flux_data = []
        for dep in dep_data["departures"]:
            vehicle, station, origin, terminus, base_time_in, time_in, base_time_out, time_out, line, network, physical_mode = None, None, None, None, None, None, None, None, None, None, None
            if dep.get("links") is not None:
                for link in dep.get("links"):
                    if link.get("type") == "line":
                        line = link.get("id")
                    elif link.get("type") == "vehicle_journey":
                        vehicle = link.get("id")
                    elif link.get("type") == "physical_mode":
                        physical_mode = link.get("id")
                    elif link.get("type") == "network":
                        network = link.get("id")
            station = dep.get("stop_point", {}).get("id")
            if dep.get("stop_date_time", {}).get("links") is not None:
                for link in dep.get("stop_date_time", {}).get("links"):
                    if link.get("rel") == "origins":
                        origin = link.get("id")
                    elif link.get("rel") == "terminus":
                        terminus = link.get("id")
            base_time_in = parse_time(dep.get("stop_date_time", {}).get("base_arrival_date_time"))
            time_in = parse_time(dep.get("stop_date_time", {}).get("arrival_date_time"))
            base_time_out = parse_time(dep.get("stop_date_time", {}).get("base_departure_date_time"))
            time_out = parse_time(dep.get("stop_date_time", {}).get("departure_date_time"))
            flux_data.append((vehicle, station, origin, terminus, base_time_in, time_in, base_time_out, time_out, line, network, physical_mode))
        for arr in arr_data["arrivals"]:
            vehicle, station, origin, terminus, base_time_in, time_in, base_time_out, time_out, line, network, physical_mode = None, None, None, None, None, None, None, None, None, None, None
            if arr.get("links") is not None:
                for link in arr.get("links"):
                    if link.get("type") == "line":
                        line = link.get("id")
                    elif link.get("type") == "vehicle_journey":
                        vehicle = link.get("id")
                    elif link.get("type") == "physical_mode":
                        physical_mode = link.get("id")
                    elif link.get("type") == "network":
                        network = link.get("id")
            station = arr.get("stop_point", {}).get("id")
            if arr.get("stop_date_time", {}).get("links") is not None:
                for link in arr.get("stop_date_time", {}).get("links"):
                    if link.get("rel") == "origins":
                        origin = link.get("id")
                    elif link.get("rel") == "terminus":
                        terminus = link.get("id")
            base_time_in = parse_time(arr.get("stop_date_time", {}).get("base_arrival_date_time"))
            time_in = parse_time(arr.get("stop_date_time", {}).get("arrival_date_time"))
            base_time_out = parse_time(arr.get("stop_date_time", {}).get("base_departure_date_time"))
            time_out = parse_time(arr.get("stop_date_time", {}).get("departure_date_time"))
            flux_data.append((vehicle, station, origin, terminus, base_time_in, time_in, base_time_out, time_out, line, network, physical_mode))

        cur.executemany("INSERT INTO flux (vehicle_journey_id, station_id, origin_id, terminus_id, base_time_in, time_in, base_time_out, time_out, line_id, network_id, physical_mode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (vehicle_journey_id, station_id) DO NOTHING",
                        flux_data)
        conn.commit()
    except Exception as e:
        logger.error(f"Ingestion failed for {area_id}: {e}")
        conn.rollback()

cur.close()
conn.close()