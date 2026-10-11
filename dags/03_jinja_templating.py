from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="03_jinja_templating",
    description="Demonstrates Airflow Jinja templating",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    show_runtime_info = BashOperator(
        task_id="show_runtime_info",
        bash_command="""
        echo "DAG: {{ dag.dag_id }}"
        echo "Task: {{ task.task_id }}"
        echo "Run ID: {{ run_id }}"
        """
    )