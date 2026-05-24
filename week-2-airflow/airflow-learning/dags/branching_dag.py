from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime   
import random 

def generate_random_number():
    v = random.randint(0, 100)
    print(f"Generated random number: {v}")
    if v >= 70:
        return 'process_good_data'
    else:
        return 'process_bad_data'

def process_good_data():
    print("Processing good data")

def process_bad_data():
    print("Processing bad data")

with DAG(
    dag_id="branching_dag",
    start_date=datetime(2026, 5, 8),
    schedule="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    generate_random_number_task = BranchPythonOperator(
        task_id="generate_random_number_task",
        python_callable=generate_random_number,
    )

    good_data_task = PythonOperator(
        task_id="process_good_data",
        python_callable=process_good_data,
    )

    bad_data_task = PythonOperator(
        task_id="process_bad_data",
        python_callable=process_bad_data,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> generate_random_number_task >> [good_data_task, bad_data_task] >> end
