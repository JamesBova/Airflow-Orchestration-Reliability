import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, get_current_context, task


logger = logging.getLogger(__name__)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="08_b_retries",
    description="Deeper look at Airflow retry behavior",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["reliability", "deep-dive"],
) as dag:

    @task
    def fail_once_then_succeed():
        context = get_current_context()
        ti = context["ti"]

        logger.info(
            "Current attempt number: %s",
            ti.try_number,
        )

        if ti.try_number == 1:
            logger.warning(
                "Intentional failure on first attempt."
            )

            raise RuntimeError(
                "First attempt intentionally failed."
            )

        logger.info(
            "Retry succeeded on attempt %s",
            ti.try_number,
        )

    @task(
        retries=0
    )
    def override_defaults():
        logger.info(
            "This task overrides the DAG default and will not retry."
        )

        raise RuntimeError(
            "Intentional failure with retries disabled."
        )

    first = fail_once_then_succeed()
    second = override_defaults()

    first >> second