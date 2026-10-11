from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, task


OUTPUT_FILE = Path("/opt/airflow/data/idempotency_demo.txt")


with DAG(
    dag_id="11_idempotency",
    description="Demonstrates idempotent task design",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task
    def write_output():
        OUTPUT_FILE.write_text(
            "Every time this task runs, it will always have the same output and not create duplicate data.\n"
        )

        print(f"Wrote output to {OUTPUT_FILE}")

    write_output()