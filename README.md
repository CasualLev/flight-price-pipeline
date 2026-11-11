# Flight Price Analysis Pipeline (Bangladesh)

Airflow pipeline: CSV -> MySQL (staging) -> validation -> transformation + KPIs -> PostgreSQL (analytics).

## Structure
- `dags/` – Airflow DAG definitions
- `dags/flight_pipeline/` – Python logic for each task
- `sql/` – table creation scripts for MySQL and PostgreSQL
- `data/raw/` – the Kaggle CSV (not committed)
- `tests/` – unit tests
- `docs/report.md` – project report

## Setup
1. Copy `.env.example` to `.env` and fill in credentials.
2. Put `Flight_Price_Dataset_of_Bangladesh.csv` in `data/raw/`.