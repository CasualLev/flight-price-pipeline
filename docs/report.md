# Flight Price Analysis Pipeline: Report

## 1. Overview

This project is an end-to-end data pipeline built with Apache Airflow. It processes the
*Flight Price Dataset of Bangladesh* (Kaggle, 57,000 bookings). The pipeline loads the raw
CSV into a MySQL staging database, validates and transforms the data, computes key
performance indicators (KPIs), and loads the results into a PostgreSQL analytics database.

**Technologies:** Apache Airflow 3.3.1 (Docker Compose), Python (pandas), MySQL (staging),
PostgreSQL (analytics), Git/GitHub.

## 2. Pipeline architecture and execution flow

```
 CSV file ──► [ingest_csv] ──► MySQL: raw_flights
                                   │
                              [validate] ──► MySQL: validated_flights
                                   │          MySQL: rejected_flights (quarantine)
                                   │
                              [transform] ──► MySQL: flights_transformed + 5 KPI tables
                                   │
                         [load_to_postgres] ──► PostgreSQL: flights_transformed + 5 KPI tables
```

**Infrastructure**
- Airflow runs in Docker containers using the official Docker Compose setup (API server,
  scheduler, DAG processor, Celery worker, and Airflow's own metadata database).
- MySQL and PostgreSQL run on the host machine. Containers reach them through
  `host.docker.internal`.
- Database connections are defined as environment variables (`AIRFLOW_CONN_MYSQL_STAGING`,
  `AIRFLOW_CONN_POSTGRES_ANALYTICS`) built from a git-ignored `.env` file, so no credentials
  appear in the code or the repository.
- Each database has a dedicated `flight_user` account with access only to its own project
  database (least privilege).

**Code structure**
- `dags/flight_price_dag.py` defines only the tasks and their order.
- `dags/flight_pipeline/` holds the logic, one module per step (`ingest.py`, `validate.py`,
  `transform.py`, `load.py`), plus `config.py` (shared settings) and `alerts.py`.
  This separation keeps the DAG readable and the logic reusable.

**Data flow between tasks:** data moves between tasks through database tables. Airflow's
XCom is used only for small metadata, such as row counts and the list of tables to load.

## 3. DAG and task descriptions

**DAG:** `flight_price_pipeline`. It is triggered manually (`schedule=None`) and has
`catchup` disabled. Every task retries **2 times** with a **1-minute** delay and calls a
failure-alert function if it still fails.

| Task | Module | What it does |
|---|---|---|
| `ingest_csv` | `ingest.py` | Reads the CSV with pandas and converts column names to snake_case (e.g. `Tax & Surcharge (BDT)` → `tax_surcharge_bdt`). Loads all rows into `raw_flights` in MySQL, then verifies that the MySQL row count equals the CSV row count and fails otherwise. |
| `validate` | `validate.py` | Runs the data-quality checks (Section 4). Writes passing rows to `validated_flights` and failing rows to `rejected_flights` with a `reject_reason`. Fails if more than 5% of rows are rejected. |
| `transform` | `transform.py` | Handles Total Fare, adds derived columns, computes the KPIs, and writes `flights_transformed` plus the KPI tables to MySQL. Returns the list of table names. |
| `load_to_postgres` | `load.py` | Reads all tables from MySQL, writes them to PostgreSQL in a single transaction, then verifies row counts in PostgreSQL. |

**Design choices**
- **Idempotency:** every task writes with `if_exists="replace"`, so re-running the DAG gives
  the same result without creating duplicate rows.
- **Consistency:** the PostgreSQL load runs inside one transaction. Either all tables are
  updated or none are, so the analytics database is never left partly updated.
- **Low latency:** data is read from MySQL before the transaction opens, which keeps the
  transaction short. Rows are inserted in batches of 1,000 (`method="multi"`).

## 4. Data validation

| Requirement | Check | Rows flagged |
|---|---|---|
| Required columns exist | `airline`, `source`, `destination`, `base_fare_bdt`, `tax_surcharge_bdt`, `total_fare_bdt` must all be present; otherwise the task stops | none missing |
| Missing / null values | Text fields must be non-empty; fare fields must not be null | 0 |
| Data types | Fares converted with `pd.to_numeric`; non-numeric values become null and are rejected | 0 |
| Negative fares | Any fare below 0 is rejected | 0 |
| Invalid cities | Source and destination must be 3-letter uppercase airport codes and must differ | 0 |

**Result:** all **57,000** rows passed (0% rejected). Rejected rows are never deleted. They
are kept in `rejected_flights` with a reason, so data problems remain traceable.

## 5. Transformation and KPIs

### Transformation
- **Total Fare:** calculated as `base_fare_bdt + tax_surcharge_bdt` **only where it is
  missing**, as the brief requires (0 rows needed this). Rows where the existing Total Fare
  differs from Base + Tax by more than 1 BDT are **kept unchanged** and flagged with
  `fare_mismatch = 1` (2,522 rows, see Section 7).
- **Derived columns:** `departure_date_time` converted to a datetime; `route`
  (e.g. `DAC-DXB`); and `season_type`, which is **Non-Peak** for "Regular" and **Peak** for
  Winter Holidays, Hajj and Eid.

### KPI definitions and results

**1. Average Fare by Airline** (`kpi_avg_fare_by_airline`): the mean `total_fare_bdt`
grouped by airline.

| Highest | Avg fare (BDT) | | Lowest | Avg fare (BDT) |
|---|---|---|---|---|
| Turkish Airlines | 75,547.27 | | Vistara | 68,108.24 |
| AirAsia | 74,534.39 | | Singapore Airlines | 68,323.93 |
| Cathay Pacific | 73,325.09 | | NovoAir | 68,350.86 |

The averages fall in a narrow band. The most expensive airline is only about 11% above the
cheapest.

**2. Seasonal Fare Variation** (`kpi_fare_by_season`, `kpi_peak_vs_nonpeak`): the mean
`total_fare_bdt` per season, and Peak vs Non-Peak.

| Season | Type | Avg fare (BDT) | Bookings | vs Regular |
|---|---|---|---|---|
| Hajj | Peak | 97,144.47 | 942 | +42.7% |
| Eid | Peak | 91,560.02 | 603 | +34.5% |
| Winter Holidays | Peak | 79,676.74 | 10,930 | +17.0% |
| Regular | Non-Peak | 68,077.27 | 44,525 | – |

**Peak fares average 81,570.14 BDT against 68,077.27 BDT non-peak: 19.8% higher.**
Religious travel seasons show the biggest increases.

**3. Booking Count by Airline** (`kpi_booking_count_by_airline`): the number of bookings
(rows) per airline. Most airlines have between 2,201 and 2,368 bookings.
**US-Bangla Airlines is the clear outlier with 4,496 bookings**, about twice the others.

**4. Most Popular Routes** (`kpi_popular_routes`): source–destination pairs ranked by
booking count (top 10).

| Rank | Route | Bookings |
|---|---|---|
| 1 | RJH → SIN | 417 |
| 2 | DAC → DXB | 413 |
| 3 | BZL → YYZ | 410 |
| 4 | CGP → BKK | 408 |
| 5 | CXB → DEL | 408 |
| 6 | BZL → JED | 407 |
| 7 | RJH → JFK | 404 |
| 8 | CGP → CXB | 404 |
| 9 | SPD → DEL | 404 |
| 10 | CGP → RJH | 403 |

The top routes are very close (403–417 bookings). Together with the even airline counts,
this suggests the dataset is synthetic or evenly sampled, so route rankings should be
interpreted with care.

## 6. Logging and alerting

- **Logging:** every module uses Python's `logging`. Airflow captures these messages in each
  task's log in the UI. Each step logs what it read, what each check found, how many rows it
  wrote, and a final summary.
- **Built-in checks that fail loudly:** row-count checks after ingestion and after the
  PostgreSQL load, and a maximum 5% reject rate in validation. A failure stops the pipeline
  so no bad data moves downstream.
- **Retries:** 2 automatic retries per task, for temporary problems such as a brief
  database outage.
- **Alerts:** when a task fails after its retries, `task_failure_alert` writes an `ALERT`
  line to the log and sends an email through Gmail SMTP with the DAG, task, run ID and
  error. SMTP credentials come from `.env`. The alert function catches its own errors, so a
  failed email can never hide the original failure.

## 7. Challenges and resolutions

| Challenge | Resolution |
|---|---|
| Airflow does not run natively on Windows. | Ran Airflow with Docker Desktop on WSL 2. Enabling WSL failed at first because PowerShell was not running as Administrator; enabling the Windows features from an elevated prompt fixed it. |
| Containers could not use `localhost` to reach the databases on the host. | Used `host.docker.internal` as the database host. |
| MySQL rejected logins from Airflow ("Access denied for 'flight_user'@'localhost'"). | Docker Desktop forwards connections so MySQL sees them as coming from `localhost`, which MySQL treats as a separate account from `flight_user@'%'`. A `flight_user@'localhost'` account was added and the passwords were aligned with `.env`. |
| Loss of access to the original development PC, with unpushed commits. | Re-cloned the repository on a new machine and recreated the missing setup files. Commits are now pushed immediately after each one. |
| **2,522 rows (4.4%) had a Total Fare that did not equal Base + Tax.** | The first version overwrote these with Base + Tax. Investigating showed every mismatched total is **exactly 1.20 × (Base + Tax)**, a deliberate 20% surcharge rather than random errors. The mismatches occur in every season (Regular 5.0%, Eid 4.0%, Hajj 3.4%, Winter Holidays 2.0%), so their cause cannot be determined from the data. The transformation was changed to **keep the source values** and flag them with `fare_mismatch`, so they can be analysed separately. |

## 8. How to run

See `README.md` for the full steps. In short: create `.env`, run the SQL setup scripts,
place the CSV in `data/raw/`, start Airflow with `docker compose up airflow-init` and then
`docker compose up -d`, open http://localhost:8080, and trigger `flight_price_pipeline`.

## 9. Possible improvements

- Schedule the DAG (for example daily) and switch to incremental loads instead of full
  table replacement.
- Add unit tests for the validation and transformation functions in `tests/`.
- Define explicit table schemas (SQL DDL) instead of relying on pandas type inference.
- Send alerts to a team channel such as Slack, and add a success summary notification.
- Investigate the source of the 20% fare surcharge with the data provider.