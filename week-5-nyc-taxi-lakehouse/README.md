## Week 5 — NYC Taxi Lakehouse
### Airflow + dbt + DuckDB Medallion Architecture

### Problem Statement
Build a production-style lakehouse pipeline that ingests 
7.4M NYC taxi trips, processes them through bronze/silver/gold 
layers, and serves analytical models — all orchestrated by Airflow.

### Architecture
Raw Parquet (7.4M rows)
↓
Airflow DAG (taxi_lakehouse_pipeline)
↓
Bronze Layer — raw ingestion + _source_file + _ingested_at
        ↓
Quality Gate (BranchPythonOperator)
        ↓
Silver Layer — cleaned, validated, typed
  - Filtered 193K bad records (2.6%)
  - Added trip_category, pickup_hour, is_rush_hour
  - dbt tests: unique, not_null, accepted_values
        ↓
Gold Layer — business models
  - fct_hourly_revenue
  - fct_trip_summary
  - fct_daily_revenue
  - dbt tests on all models

### Data Quality
- Bronze → Silver: filtered negative fares, zero passengers,
  future timestamps, fare outliers >$1000
- dbt tests: unique, not_null, accepted_values on all models
- Quality gate: pipeline halts if bronze row count < 1M

### dbt lineage


### Key Design Decisions
- Full refresh on bronze — raw data never modified
- Table materialization on silver/gold — frequently queried
- BranchPythonOperator quality gate — bad data never reaches silver
- Absolute container paths in profiles.yml for Docker compatibility

### dbt Lineage
[paste your lineage screenshot here]

image.png

---

## Running Locally

### 1. Project setup

```bash
mkdir week-5-nyc-taxi-lakehouse && cd week-5-nyc-taxi-lakehouse

mkdir -p data/raw
mkdir -p airflow/dags
mkdir dbt
```

### 2. Download NYC Taxi data

```bash
curl -L https://github.com/duckdb/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet \
  -o data/raw/taxi_2019_04.parquet
```

### 3. Set up dbt locally

```bash
cd dbt
python -m venv venv
source venv/bin/activate
pip install dbt-duckdb

dbt init taxi_lakehouse
```

Run and test models individually:

```bash
dbt run --select stg_taxi_trips
dbt test
```

### 4. Run the full pipeline via Airflow (Docker)

The Dockerfile is in `week-5-nyc-taxi-lakehouse/airflow/`. Build the image first (one-time):

```bash
cd airflow
docker build -t airflow-standalone-custom:1 .
cd ..
```

Then start the container from the project root:

```bash
docker run -d -p 8080:8080 \
  -v $(pwd)/airflow/dags:/opt/airflow/dags \
  -v $(pwd)/data:/opt/airflow/data \
  -v $(pwd)/dbt/taxi_lakehouse:/opt/airflow/dbt \
  --name airflow-standalone-custom \
  airflow-standalone-custom:1 standalone
```

Open [http://localhost:8080](http://localhost:8080) and trigger the `taxi_lakehouse_pipeline` DAG.

> **Note:** The container uses `/opt/airflow/data/taxi_lakehouse.duckdb` as the DuckDB path.
> When running dbt locally (outside Docker), update `profiles.yml` or set:
> ```bash
> export DUCKDB_PATH=/path/to/your/local/taxi_lakehouse.duckdb
> ```

---

### Stack
Apache Airflow, dbt Core, DuckDB, Docker, Python

### What I'd do differently at scale
- Replace DuckDB with BigQuery or Snowflake
- Use KubernetesPodOperator for dbt instead of BashOperator
- Add Great Expectations as ingestion quality gate
- Add incremental loads on silver/gold instead of full refresh