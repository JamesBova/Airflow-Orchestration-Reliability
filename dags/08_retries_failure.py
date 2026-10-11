from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="08_retries_failure",
    description="Demonstrates retries with exponential backoff",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task(
        retries=4,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )
    def unreliable_task():
        raise Exception("Intentional failure for retry demonstration")

    unreliable_task()