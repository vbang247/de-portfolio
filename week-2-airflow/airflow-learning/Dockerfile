# Lightweight: extend Airflow with dbt-duckdb only (no extra apt packages).
# dbt-duckdb ships wheels for linux/amd64 — no build-essential needed in most cases.
FROM apache/airflow:2.8.1-python3.11
USER airflow
RUN pip install --no-cache-dir dbt-duckdb kafka-python duckdb