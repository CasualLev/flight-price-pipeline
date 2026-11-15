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
## Setup
1. Create a file named `.env` in the project root with these variables
   (use your own passwords):
   ```
   AIRFLOW_UID=50000

   MYSQL_HOST=host.docker.internal
   MYSQL_PORT=3306
   MYSQL_DB=flight_staging
   MYSQL_USER=flight_user
   MYSQL_PASSWORD=your_password

   POSTGRES_HOST=host.docker.internal
   POSTGRES_PORT=5432
   POSTGRES_DB=flight_analytics
   POSTGRES_USER=flight_user
   POSTGRES_PASSWORD=your_password

   ALERT_EMAIL=you@example.com
   ```
2. Run `sql/mysql/setup.sql` in MySQL and `sql/postgres/setup.sql` in PostgreSQL
   (replace `change_me` with the passwords from `.env`).
3. Put `Flight_Price_Dataset_of_Bangladesh.csv` in `data/raw/`.
4. Start Airflow:
   ```
   docker compose up airflow-init
   docker compose up -d
   ```
5. Open http://localhost:8080 (login: airflow / airflow).