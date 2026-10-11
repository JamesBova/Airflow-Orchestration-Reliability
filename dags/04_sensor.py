import os
from datetime import datetime, timezone

from airflow.sdk import DAG, task
from airflow.providers.standard.sensors.python import PythonSensor


FILE_PATH = "/opt/airflow/dags/sensor_test.txt"


def file_exists():
    return os.path.exists(FILE_PATH)


with DAG(
    dag_id="04_sensor",
    description="Demonstrates an Airflow Sensor",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability"],
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=file_exists,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    @task
    def process_file():
        print("The file exists. Continuing processing.")

    wait_for_file >> process_file()