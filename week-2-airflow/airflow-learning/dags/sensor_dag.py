from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from airflow.models import Variable

def process_file():
    v = Variable.get("watch_file_path")
    print(f"Found file: {v}")
    print(f"Processing file: {v}")

with DAG(
    dag_id="file_sensor_dag",
    start_date=datetime(2026, 5, 8),
    schedule="@daily",
    catchup=False
) as dag:
    watch_file_path = FileSensor(
        task_id="watch_file_path",
        filepath="/opt/airflow/dags/trigger.txt",
        poke_interval=10,
        timeout=60,
        mode="poke"
    )

    file_process = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )

    watch_file_path >> file_process