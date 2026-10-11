from datetime import datetime, timezone

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="16_secrets",
    description="Demonstrates connection credentials from environment variables",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task
    def test_secret_connection():
        hook = PostgresHook(
            postgres_conn_id="demo_postgres_env"
        )

        result = hook.get_first(
            "SELECT current_database(), current_user;"
        )

        print(f"Database: {result[0]}")
        print(f"User: {result[1]}")

    test_secret_connection()