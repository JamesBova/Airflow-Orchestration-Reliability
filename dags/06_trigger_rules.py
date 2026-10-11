from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="06_trigger_rules",
    description="Demonstrates trigger rules after branching",
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
        print("Processing HIGH path")

    @task
    def process_low():
        print("Processing LOW path")

    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("One valid branch completed. Finishing DAG.")

    branch = choose_path()

    high = process_high()
    low = process_low()

    end = finish()

    branch >> [high, low]
    [high, low] >> end