from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from flight_pipeline.alerts import task_failure_alert
from flight_pipeline.ingest import load_csv_to_mysql
from flight_pipeline.validate import validate_staging_data
from flight_pipeline.load import load_to_postgres
from flight_pipeline.transform import transform_and_compute_kpis

default_args = {
    "owner": "data-team",
    "retries": 2,                          # retry twice before failing
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="flight_price_pipeline",
    schedule=None,                         # run manually for now
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["flights", "etl"],
)
def flight_price_pipeline():

    @task
    def ingest_csv():
        return load_csv_to_mysql()

    @task
    def validate():
        return validate_staging_data()

    @task
    def transform():
        return transform_and_compute_kpis()

    @task
    def load(tables):
        return load_to_postgres(tables)

    ingested = ingest_csv()
    validated = validate()
    tables = transform()

    ingested >> validated >> tables
    load(tables)


flight_price_pipeline()