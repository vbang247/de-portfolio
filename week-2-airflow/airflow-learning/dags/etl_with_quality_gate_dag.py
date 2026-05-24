from itertools import count
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def extract():
    data = [
        {"id": 1, "amount": 100, "status": "completed"},
        {"id": 2, "amount": None, "status": "placed"},
        {"id": 3, "amount": 50, "status": "completed"},
    ]
    print("Extraction done")
    return data

def check_quality(**context):
    data = context['ti'].xcom_pull(task_ids="extract_data")
    print(f"Data length: {len(data)}")
    null_count = [row for row in data if row['amount'] is None ]
    print(f"Null count: {len(null_count)}")
    null_pct = ( len(null_count) / len(data) ) * 100
    if null_pct >= 35:
        return "process_data"
    else:
        return "handle_failure"

def process_data(**context):
    data = context['ti'].xcom_pull(task_ids="extract_data")
    clean = [row for row in data if row['amount'] is not None]
    print(f"Processed {len(clean)} records")
    return clean

def handle_failure(**context):
    print("Quality check failed — alerting team!")
    # In production: Slack API call here

with DAG(
    dag_id="etl_with_quality_gate_tag",
    start_date=datetime(2026, 5, 8),
    schedule="@daily",
    catchup=False
)as dag:
    start = EmptyOperator(task_id="start_run")

    extract_data=PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )

    check_quality_gate = BranchPythonOperator(
        task_id="check_quality",
        python_callable=check_quality
    )

    process_good_data = PythonOperator(
        task_id="process_data",
        python_callable=process_data

    )

    handle_quality_failure=PythonOperator(
        task_id="handle_failure",
        python_callable=handle_failure
    )

    end = EmptyOperator(
        task_id="end_run",
        trigger_rule="none_failed_min_one_success"
    )

    start >> extract_data >> check_quality_gate >> [process_good_data, handle_quality_failure] >> end


        