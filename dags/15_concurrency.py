from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="15_concurrency",
    description="Demonstrates DAG-level task concurrency",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_tasks=2,
    tags=["reliability"],
) as dag:

    task_a = BashOperator(
        task_id="task_a",
        bash_command='echo "Starting A"; sleep 10; echo "Finished A"',
    )

    task_b = BashOperator(
        task_id="task_b",
        bash_command='echo "Starting B"; sleep 10; echo "Finished B"',
    )

    task_c = BashOperator(
        task_id="task_c",
        bash_command='echo "Starting C"; sleep 10; echo "Finished C"',
    )