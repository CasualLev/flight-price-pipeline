import logging

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_pipeline.config import (
    MAX_REJECT_RATE,
    MYSQL_CONN_ID,
    REJECTED_TABLE,
    STAGING_TABLE,
    VALIDATED_TABLE,
)

log = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "airline", "source", "destination",
    "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt",
]
TEXT_COLUMNS = ["airline", "source", "destination"]
FARE_COLUMNS = ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]
AIRPORT_CODE_PATTERN = r"^[A-Z]{3}$"   # e.g. DAC, CGP, DXB


def validate_staging_data():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(f"SELECT * FROM {STAGING_TABLE}", engine)
    total = len(df)
    log.info("Validating %d rows from '%s'", total, STAGING_TABLE)

    # Check 1: all required columns exist. If not, stop immediately.
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    log.info("All required columns present")

    # Clean up text and number columns before checking them.
    for col in TEXT_COLUMNS:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["source"] = df["source"].str.upper()
    df["destination"] = df["destination"].str.upper()
    for col in FARE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")  # bad values -> empty

    df["reject_reason"] = ""

    def flag(mask, reason):
        """Mark rows that break a rule (keep only the first reason per row)."""
        new_rejects = mask & (df["reject_reason"] == "")
        df.loc[new_rejects, "reject_reason"] = reason
        log.info("Check '%s': %d rows flagged", reason, int(mask.sum()))

    # Check 2: non-empty text fields
    for col in TEXT_COLUMNS:
        flag(df[col] == "", f"missing {col}")

    # Check 3: fares must be numeric (not null)
    for col in FARE_COLUMNS:
        flag(df[col].isna(), f"missing or non-numeric {col}")

    # Check 4: no negative fares
    flag((df[FARE_COLUMNS] < 0).any(axis=1), "negative fare")

    # Check 5: valid airport codes, and source != destination
    flag(~df["source"].str.match(AIRPORT_CODE_PATTERN), "invalid source code")
    flag(~df["destination"].str.match(AIRPORT_CODE_PATTERN), "invalid destination code")
    flag(df["source"] == df["destination"], "source equals destination")

    # Split into good and bad rows.
    valid = df[df["reject_reason"] == ""].drop(columns="reject_reason")
    rejected = df[df["reject_reason"] != ""]

    valid.to_sql(VALIDATED_TABLE, engine, if_exists="replace", index=False, chunksize=5000)
    rejected.to_sql(REJECTED_TABLE, engine, if_exists="replace", index=False, chunksize=5000)

    reject_rate = len(rejected) / total if total else 1
    log.info(
        "Validation done: %d valid, %d rejected (%.2f%%)",
        len(valid), len(rejected), reject_rate * 100,
    )

    if reject_rate > MAX_REJECT_RATE:
        raise ValueError(
            f"Reject rate {reject_rate:.2%} is above the {MAX_REJECT_RATE:.0%} limit"
        )

    return {"total": total, "valid": len(valid), "rejected": len(rejected)}