import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


with DAG(
    dag_id="01_b_connections_hooks",
    description="Deeper look at Airflow Connections and Hooks",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    @task
    def inspect_ui_connection():
        connection_id = "demo_postgres"

        hook = PostgresHook(
            postgres_conn_id=connection_id
        )

        # Ask Airflow for the Connection object itself.
        connection = hook.get_connection(connection_id)

        logger.info("Connection ID: %s", connection.conn_id)
        logger.info("Connection type: %s", connection.conn_type)
        logger.info("Host: %s", connection.host)
        logger.info("Database/schema: %s", connection.schema)
        logger.info("Login: %s", connection.login)
        logger.info("Port: %s", connection.port)

        # Do NOT log connection.password.

        result = hook.get_first(
            """
            SELECT
                current_database(),
                current_user;
            """
        )

        logger.info("Connected database: %s", result[0])
        logger.info("Connected user: %s", result[1])

    @task
    def inspect_environment_connection():
        connection_id = "demo_postgres_env"

        hook = PostgresHook(
            postgres_conn_id=connection_id
        )

        connection = hook.get_connection(connection_id)

        logger.info("Connection ID: %s", connection.conn_id)
        logger.info("Connection type: %s", connection.conn_type)
        logger.info("Host: %s", connection.host)
        logger.info("Database/schema: %s", connection.schema)
        logger.info("Login: %s", connection.login)
        logger.info("Port: %s", connection.port)

        result = hook.get_first(
            """
            SELECT
                current_database(),
                current_user;
            """
        )

        logger.info("Connected database: %s", result[0])
        logger.info("Connected user: %s", result[1])

    ui_connection = inspect_ui_connection()
    environment_connection = inspect_environment_connection()