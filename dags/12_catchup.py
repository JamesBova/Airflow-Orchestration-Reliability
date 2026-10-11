from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="12_catchup",
    description="Demonstrates Airflow catchup behavior",
    schedule="@daily",
    start_date=datetime(2026, 10, 7, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=1,
    tags=["reliability"],
) as dag:

    process_day = BashOperator(
        task_id="process_day",
        bash_command='echo "Processing logical date: {{ ds }}"',
    )