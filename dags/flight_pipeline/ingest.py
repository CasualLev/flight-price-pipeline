import logging
import re

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_pipeline.config import CSV_PATH, MYSQL_CONN_ID, STAGING_TABLE

log = logging.getLogger(__name__)


def clean_column_name(name):
    """'Tax & Surcharge (BDT)' -> 'tax_surcharge_bdt' (safe for SQL)."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def load_csv_to_mysql():
    log.info("Reading CSV from %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    log.info("CSV has %d rows and %d columns", len(df), len(df.columns))

    df.columns = [clean_column_name(c) for c in df.columns]
    log.info("Columns: %s", list(df.columns))

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # 'replace' makes the task safe to re-run: it rebuilds the table each time.
    df.to_sql(STAGING_TABLE, engine, if_exists="replace", index=False, chunksize=5000)

    # Check that every row from the CSV made it into MySQL.
    loaded = hook.get_first(f"SELECT COUNT(*) FROM {STAGING_TABLE}")[0]
    if loaded != len(df):
        raise ValueError(f"Row count mismatch: CSV={len(df)}, MySQL={loaded}")

    log.info("Loaded %d rows into MySQL table '%s'", loaded, STAGING_TABLE)
    return loaded