from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    data = [1, 2, 3, 4, 5]
    print(f"Extracted data: {data}")
    return data

def transform(**context):
    data = context['ti'].xcom_pull(task_ids='extract_task')
    transformed_data = [x * 2 for x in data]
    print(f"Transformed data: {transformed_data}")
    return transformed_data

def load(**context):
    data = context['ti'].xcom_pull(task_ids='transform_task')
    print(f"Loading data: {data}")

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
