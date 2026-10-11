from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="05_branching",
    description="Demonstrates conditional branching",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task.branch
    def choose_path():
        value = 75

        if value >= 50:
            return "process_high"

        return "process_low"

    @task
    def process_high():
        print("Taking the HIGH path")

    @task
    def process_low():
        print("Taking the LOW path")

    branch = choose_path()

    high = process_high()
    low = process_low()

    branch >> [high, low]