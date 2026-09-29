# Contains the script to fill the stop_areas, stations, lines and networks tables

# Packages imported
import os
import psycopg2

from ingestion_functions import BASE_URL, fetch_all_pages


conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
)
cur = conn.cursor()

cur.executemany("INSERT INTO stop_areas (area_id, name, lon, lat) VALUES (%s, %s, %s, %s)",
                fetch_all_pages(BASE_URL, "stop_areas"))
conn.commit()

cur.executemany("INSERT INTO stations (station_id, station_name, area_id, lon, lat) VALUES (%s, %s, %s, %s, %s)",
                fetch_all_pages(BASE_URL, "stop_points"))
conn.commit()

cur.executemany("INSERT INTO lines (line_id, line_name) VALUES (%s, %s)",
                fetch_all_pages(BASE_URL, "lines"))
conn.commit()

cur.executemany("INSERT INTO networks (network_id, network_name) VALUES (%s, %s)",
                fetch_all_pages(BASE_URL, "networks"))
conn.commit()

cur.close()
conn.close()
