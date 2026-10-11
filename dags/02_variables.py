from datetime import datetime, timezone

from airflow.sdk import DAG, Variable, task


with DAG(
    dag_id="02_variables",
    description="Demonstrates Airflow Variables",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task
    def show_environment():
        environment = Variable.get("environment_name")

        print(f"Environment: {environment}")

    show_environment()