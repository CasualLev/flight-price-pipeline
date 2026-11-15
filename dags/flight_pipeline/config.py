# Central settings so no file has hard-coded names scattered around.
CSV_PATH = "/opt/airflow/data/raw/Flight_Price_Dataset_of_Bangladesh.csv"

MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

STAGING_TABLE = "raw_flights"

VALIDATED_TABLE = "validated_flights"
REJECTED_TABLE = "rejected_flights"

# If more than this share of rows is rejected, fail the task (and alert).
MAX_REJECT_RATE = 0.05