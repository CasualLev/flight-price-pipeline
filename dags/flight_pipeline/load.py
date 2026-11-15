import logging

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from flight_pipeline.config import MYSQL_CONN_ID, POSTGRES_CONN_ID

log = logging.getLogger(__name__)


def load_to_postgres(tables):
    mysql_engine = MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_sqlalchemy_engine()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_engine = pg_hook.get_sqlalchemy_engine()

    # 1. Read everything from MySQL first, so the PostgreSQL transaction stays short.
    data = {}
    for table in tables:
        data[table] = pd.read_sql(f"SELECT * FROM {table}", mysql_engine)
        log.info("Read %d rows from MySQL table '%s'", len(data[table]), table)

    # 2. Write all tables in ONE transaction: all succeed, or nothing changes.
    with pg_engine.begin() as conn:
        for table, df in data.items():
            df.to_sql(table, conn, if_exists="replace", index=False,
                      chunksize=1000, method="multi")
            log.info("Wrote '%s' to PostgreSQL", table)

    # 3. Verify row counts match between MySQL and PostgreSQL.
    for table, df in data.items():
        count = pg_hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        if count != len(df):
            raise ValueError(f"Row count mismatch for '{table}': MySQL={len(df)}, PostgreSQL={count}")

    log.info("Loaded %d tables into PostgreSQL, all row counts verified", len(data))
    return {table: len(df) for table, df in data.items()}