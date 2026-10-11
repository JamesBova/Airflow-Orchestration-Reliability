import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, Variable, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


with DAG(
    dag_id="16_b_secrets",
    description="Deeper look at Airflow environment-based secrets",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    @task
    def inspect_environment_connection():

        hook = PostgresHook(
            postgres_conn_id="demo_postgres_env"
        )

        connection = hook.get_connection(
            "demo_postgres_env"
        )

        logger.info(
            "Connection ID: %s",
            connection.conn_id,
        )

        logger.info(
            "Host: %s",
            connection.host,
        )

        logger.info(
            "Database: %s",
            connection.schema,
        )

        logger.info(
            "Login: %s",
            connection.login,
        )

        # Never log connection.password.

        result = hook.get_first(
            "SELECT current_database(), current_user;"
        )

        logger.info(
            "Connected database: %s",
            result[0],
        )

        logger.info(
            "Connected user: %s",
            result[1],
        )

    @task
    def inspect_environment_variable():

        value = Variable.get(
            "secret_source_test"
        )

        logger.info(
            "secret_source_test = %s",
            value,
        )

    inspect_environment_connection()
    inspect_environment_variable()