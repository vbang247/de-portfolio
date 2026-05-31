from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from bronze_verify import verify_data
from bronze_ingest import ingest_raw_data
from datetime import datetime
import duckdb

def validate_bronze(db_path):
    conn = duckdb.connect(db_path)
    count = conn.execute("SELECT COUNT(*) from bronze.taxi_trips_raw").fetchone()[0]
    if count < 10000:
        print("Count of rows in Bronze not correct")
        return "handle_failure"
    print("Bronze data loaded")
    return "seed_dbt"

def handle_failure():
    print("Issues with Bronze data, alerting team!")

with DAG(
    dag_id="taxi_lakehouse_pipeline",
    schedule="@daily",
    catchup=False,
    start_date=datetime(2026, 5, 25)
) as dag:
    start = EmptyOperator(task_id="start") 

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="python /opt/airflow/dags/bronze_ingest.py",
        env={
            "DB_PATH": "/opt/airflow/data/taxi_lakehouse.duckdb",
            "RAW_DATA_PATH": "/opt/airflow/data/raw/taxi_2019_04.parquet"
        },
        append_env=True
    )

    verify_bronze = PythonOperator(
        task_id="verify_bronze",
        python_callable=verify_data,
        op_kwargs={
            'db_path': '/opt/airflow/data/taxi_lakehouse.duckdb'
        }
    )

    quality_gate = BranchPythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
        op_kwargs={
            'db_path': '/opt/airflow/data/taxi_lakehouse.duckdb'
        }
    )
    failure_task = PythonOperator(
        task_id="handle_failure",
        python_callable=handle_failure
    )
    seed_dbt = BashOperator(
        task_id="seed_dbt",
        bash_command="cd /opt/airflow/dbt && dbt seed --profiles-dir /opt/airflow/dbt"
    )

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver --profiles-dir /opt/airflow/dbt",
        trigger_rule="none_failed_min_one_success"
    )


    end = EmptyOperator(task_id="end")

    start >> ingest_bronze >> verify_bronze >> quality_gate >> [failure_task, seed_dbt] >> run_silver >> end