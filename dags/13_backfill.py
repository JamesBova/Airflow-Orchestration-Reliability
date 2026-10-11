from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="13_backfill",
    description="Demonstrates manually backfilling historical DAG runs",
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    process_day = BashOperator(
        task_id="process_day",
        bash_command='echo "Backfilling logical date: {{ ds }}"',
    )