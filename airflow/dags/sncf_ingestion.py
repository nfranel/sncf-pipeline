# Contains the dag functions for Airflow

# imports
from ingestion.periodic_ingestion import run_ingest_disruptions, run_ingest_flux
from ingestion.populate_permanent import run_ingest_permanent_tables
from datetime import datetime
from airflow.sdk import dag, task

ingestion_tag = ['airflow_ingestion']
catchup = False
start_date=datetime(2026, 10, 1)
max_active_runs = 1
default_args = {"retries": 0}
perma_tables_schedule = "@daily"
ingestion_schedule = "*/30 * * * *" # Ingestion every 30 minutes
# This str object is a cron, which has the following struture (https://en.wikipedia.org/wiki/Cron#CRON_expression)
# * * * * * <command to execute>
# | | | | |
# | | | | day of the week (0–6) (Sunday to Saturday;
# | | | month (1–12)             7 is also Sunday on some systems)
# | | day of the month (1–31)
# | hour (0–23)
# minute (0–59)
# use */x to make it every x min/hours/days, etc

@dag(dag_id="sncf_perma_tables",
     schedule=perma_tables_schedule,
     start_date=start_date,
     catchup=catchup,
     max_active_runs=max_active_runs,
     default_args=default_args,
     tags=ingestion_tag)
def perma_tables_dag():
    @task(pool="navitia_api")
    def ingest_perma_tables():
        return run_ingest_permanent_tables()

    ingest_perma_tables()


@dag(dag_id="sncf_ingestion",
     schedule=ingestion_schedule,
     start_date=start_date,
     catchup=catchup,
     max_active_runs = max_active_runs,
     default_args=default_args,
     tags=ingestion_tag)
def ingestion_dag():
    @task(pool="navitia_api")
    def ingest_disruptions():
        return run_ingest_disruptions()

    @task(pool="navitia_api")
    def ingest_flux():
        return run_ingest_flux()

    ingest_disruptions() >> ingest_flux()

perma_tables_dag()
ingestion_dag()