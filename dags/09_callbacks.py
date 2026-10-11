from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


def handle_retry(context):
    print("CALLBACK: Task failed and Airflow is going to retry it.")


def handle_failure(context):
    print("CALLBACK: Task has exhausted its retries and permanently failed.")


with DAG(
    dag_id="09_callbacks",
    description="Demonstrates retry and failure callbacks",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task(
        retries=1,
        retry_delay=timedelta(seconds=10),
        on_retry_callback=handle_retry,
        on_failure_callback=handle_failure,
    )
    def unreliable_task():
        raise Exception("Intentional failure")

    unreliable_task()