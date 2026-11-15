import logging

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_pipeline.config import (
    MYSQL_CONN_ID,
    NON_PEAK_SEASONS,
    TRANSFORMED_TABLE,
    VALIDATED_TABLE,
)

log = logging.getLogger(__name__)


def transform_and_compute_kpis():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(f"SELECT * FROM {VALIDATED_TABLE}", engine)
    log.info("Transforming %d validated rows", len(df))

    # --- Transformation 1: Total Fare = Base Fare + Tax & Surcharge ---
    computed = df["base_fare_bdt"] + df["tax_surcharge_bdt"]
    wrong = df["total_fare_bdt"].isna() | ((df["total_fare_bdt"] - computed).abs() > 1)
    df.loc[wrong, "total_fare_bdt"] = computed[wrong]
    log.info("Total fare recalculated for %d rows", int(wrong.sum()))

    # --- Transformation 2: enrich with useful columns ---
    df["departure_date_time"] = pd.to_datetime(df["departure_date_time"], errors="coerce")
    df["route"] = df["source"] + "-" + df["destination"]
    df["season_type"] = "Peak"
    df.loc[df["seasonality"].isin(NON_PEAK_SEASONS), "season_type"] = "Non-Peak"
    log.info("Seasons found: %s", df["seasonality"].value_counts().to_dict())

    # --- KPIs ---
    kpis = {}

    kpis["kpi_avg_fare_by_airline"] = (
        df.groupby("airline", as_index=False)
        .agg(avg_total_fare=("total_fare_bdt", "mean"))
        .round(2)
        .sort_values("avg_total_fare", ascending=False)
    )

    kpis["kpi_booking_count_by_airline"] = (
        df.groupby("airline", as_index=False)
        .size()
        .rename(columns={"size": "booking_count"})
        .sort_values("booking_count", ascending=False)
    )

    kpis["kpi_fare_by_season"] = (
        df.groupby(["season_type", "seasonality"], as_index=False)
        .agg(avg_total_fare=("total_fare_bdt", "mean"),
             booking_count=("total_fare_bdt", "size"))
        .round(2)
        .sort_values("avg_total_fare", ascending=False)
    )

    kpis["kpi_peak_vs_nonpeak"] = (
        df.groupby("season_type", as_index=False)
        .agg(avg_total_fare=("total_fare_bdt", "mean"),
             booking_count=("total_fare_bdt", "size"))
        .round(2)
    )

    routes = (
        df.groupby(["source", "destination"], as_index=False)
        .size()
        .rename(columns={"size": "booking_count"})
        .sort_values("booking_count", ascending=False)
        .head(10)
    )
    routes["rank"] = range(1, len(routes) + 1)
    kpis["kpi_popular_routes"] = routes

    # --- Save everything to MySQL (ready to be loaded into PostgreSQL) ---
    df.to_sql(TRANSFORMED_TABLE, engine, if_exists="replace", index=False, chunksize=5000)
    for name, table in kpis.items():
        table.to_sql(name, engine, if_exists="replace", index=False)
        log.info("KPI '%s': %d rows", name, len(table))

    log.info("Peak vs non-peak:\n%s", kpis["kpi_peak_vs_nonpeak"].to_string(index=False))

    # Return the table names so the load task knows what to copy.
    return [TRANSFORMED_TABLE] + list(kpis.keys())