from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="14_pools_concurrency",
    description="Demonstrates Airflow pools and concurrency control",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    task_a = BashOperator(
        task_id="task_a",
        bash_command='echo "Starting A"; sleep 10; echo "Finished A"',
        pool="demo_pool",
    )

    task_b = BashOperator(
        task_id="task_b",
        bash_command='echo "Starting B"; sleep 10; echo "Finished B"',
        pool="demo_pool",
    )

    task_c = BashOperator(
        task_id="task_c",
        bash_command='echo "Starting C"; sleep 10; echo "Finished C"',
        pool="demo_pool",
    )