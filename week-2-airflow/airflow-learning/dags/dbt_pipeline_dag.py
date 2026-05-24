from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = "/opt/airflow/dbt"
with DAG(
    dag_id="dbt_pipeline_dag",
    start_date=datetime(2026, 5, 8),
    schedule="@daily",
    catchup=False
) as dag:
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir {DBT_DIR}"
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}"
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}"
    )
    dbt_seed >> dbt_run >> dbt_test