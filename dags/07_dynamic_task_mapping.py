from datetime import datetime, timezone
from airflow.sdk import DAG, task

with DAG(
    dag_id="07_dynamic_task_mapping",
    description="Demonstrates dynamic task mapping and downstream dependency",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task
    def get_files():
        return [
            "transactions.csv",
            "accounts.csv",
            "customers.csv",
        ]

    @task
    def process_file(file_name):
        print(f"Processing {file_name}")

    @task
    def summarize_results():
        print("All mapped file-processing tasks completed successfully.")

    files = get_files()

    processed_files = process_file.expand(
        file_name=files
    )

    processed_files >> summarize_results()