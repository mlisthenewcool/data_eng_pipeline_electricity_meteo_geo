from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_working",
    description="Un DAG simple pour tester que tout fonctionne",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Déclenchement manuel uniquement
    catchup=False,
) as dag:
    task = BashOperator(
        task_id="echo_working",
        bash_command='echo "working!"',
    )
