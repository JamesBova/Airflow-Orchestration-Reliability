import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, get_current_context, task


logger = logging.getLogger(__name__)


with DAG(
    dag_id="17_xcom_task_instance",
    description="Demonstrates implicit and explicit XCom with TaskInstance",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    # -----------------------------------------------------
    # IMPLICIT XCOM
    # TaskFlow automatically stores the return value.
    # -----------------------------------------------------

    @task
    def produce_number():
        return 10

    @task
    def use_number(number):
        logger.info("Implicit XCom value: %s", number)

    # -----------------------------------------------------
    # EXPLICIT XCOM PUSH
    # -----------------------------------------------------

    @task
    def produce_metadata():
        context = get_current_context()

        ti = context["ti"]

        logger.info("Current task ID: %s", ti.task_id)
        logger.info("Current DAG ID: %s", ti.dag_id)
        logger.info("Current run ID: %s", ti.run_id)

        ti.xcom_push(
            key="row_count",
            value=250,
        )

        ti.xcom_push(
            key="file_name",
            value="transactions.csv",
        )

    # -----------------------------------------------------
    # EXPLICIT XCOM PULL
    # -----------------------------------------------------

    @task
    def consume_metadata():
        context = get_current_context()

        ti = context["ti"]

        row_count = ti.xcom_pull(
            task_ids="produce_metadata",
            key="row_count",
        )

        file_name = ti.xcom_pull(
            task_ids="produce_metadata",
            key="file_name",
        )

        logger.info("File name: %s", file_name)
        logger.info("Row count: %s", row_count)

    # -----------------------------------------------------
    # DAG FLOW
    # -----------------------------------------------------

    number = produce_number()

    implicit_consumer = use_number(number)

    metadata_producer = produce_metadata()
    metadata_consumer = consume_metadata()

    metadata_producer >> metadata_consumer