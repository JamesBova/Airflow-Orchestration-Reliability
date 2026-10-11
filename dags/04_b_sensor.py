import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, PokeReturnValue, task


logger = logging.getLogger(__name__)

READY_FILE = Path("/opt/airflow/data/sensor_ready.txt")


with DAG(
    dag_id="04_b_sensor",
    description="Deeper look at Airflow sensors",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    @task.sensor(
        poke_interval=10,
        timeout=120,
        mode="reschedule",
    )
    def wait_for_file() -> PokeReturnValue:
        exists = READY_FILE.exists()

        logger.info(
            "Checking for %s. Exists=%s",
            READY_FILE,
            exists,
        )

        if exists:
            return PokeReturnValue(
                is_done=True,
                xcom_value=str(READY_FILE),
            )

        return PokeReturnValue(
            is_done=False
        )

    @task
    def process_file(file_path):
        logger.info(
            "Sensor found file: %s",
            file_path,
        )

    file_path = wait_for_file()

    process_file(file_path)