import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, task


logger = logging.getLogger(__name__)


with DAG(
    dag_id="10_failure_logging",
    description="Demonstrates task logging and failure handling",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    @task
    def process_data():
        logger.info("Starting data processing")

        try:
            value = 10 / 0

            logger.info(f"Result: {value}")

        except Exception:
            logger.exception("Data processing failed")
            raise

        finally:
            logger.info("Processing attempt finished")

    process_data()