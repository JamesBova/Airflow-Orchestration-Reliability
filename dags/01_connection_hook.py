from datetime import datetime, timezone

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="01_connection_hook",
    description="Demonstrates Airflow Connections and Hooks",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task
    def test_postgres_connection():
        hook = PostgresHook(postgres_conn_id="demo_postgres")

        result = hook.get_first("SELECT current_database(), current_user;")

        print(f"Database: {result[0]}")
        print(f"User: {result[1]}")

    test_postgres_connection()