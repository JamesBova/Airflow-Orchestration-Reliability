import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, Variable, get_current_context, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor


logger = logging.getLogger(__name__)

READY_FILE = Path("/opt/airflow/data/capstone_ready.txt")
OUTPUT_DIR = Path("/opt/airflow/data/capstone_output")


# ---------------------------------------------------------
# Sensor callable
# ---------------------------------------------------------

def input_is_ready():
    return READY_FILE.exists()


# ---------------------------------------------------------
# Callbacks
# ---------------------------------------------------------

def handle_retry(context):
    task_instance = context.get("task_instance")

    logger.warning(
        "RETRY CALLBACK: %s will be retried.",
        task_instance.task_id if task_instance else "task",
    )


def handle_failure(context):
    task_instance = context.get("task_instance")

    logger.error(
        "FAILURE CALLBACK: %s permanently failed.",
        task_instance.task_id if task_instance else "task",
    )


with DAG(
    dag_id="18_orchestration_reliability_capstone",
    description="Capstone demonstrating Airflow orchestration and reliability concepts",
    schedule="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["reliability", "capstone"],
) as dag:

    # -----------------------------------------------------
    # SENSOR
    # Wait until an external condition is true.
    # -----------------------------------------------------

    wait_for_input = PythonSensor(
        task_id="wait_for_input",
        python_callable=input_is_ready,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    # -----------------------------------------------------
    # JINJA + OPERATOR
    # -----------------------------------------------------

    show_run_info = BashOperator(
        task_id="show_run_info",
        bash_command="""
        echo "DAG: {{ dag.dag_id }}"
        echo "Run ID: {{ run_id }}"
        echo "Logical Date: {{ ds }}"
        """,
    )

    # -----------------------------------------------------
    # CONNECTION + HOOK
    # Uses AIRFLOW_CONN_DEMO_POSTGRES_ENV from .env
    # -----------------------------------------------------

    @task
    def check_database():
        hook = PostgresHook(
            postgres_conn_id="demo_postgres_env"
        )

        result = hook.get_first(
            "SELECT current_database(), current_user;"
        )

        logger.info("Database: %s", result[0])
        logger.info("Database user: %s", result[1])

    # -----------------------------------------------------
    # VARIABLE
    # -----------------------------------------------------

    @task
    def get_environment():
        environment = Variable.get(
            "environment_name",
            default="development",
        )

        logger.info("Environment: %s", environment)

        return environment

    # -----------------------------------------------------
    # BRANCHING / CONDITIONAL PROCESSING
    # -----------------------------------------------------

    @task.branch
    def choose_path(environment):
        if environment == "development":
            logger.info("Development environment: processing files.")
            return "get_files"

        logger.info(
            "Environment is %s. Skipping file processing.",
            environment,
        )

        return "skip_processing"

    @task
    def skip_processing():
        logger.info("File processing was skipped.")

    # -----------------------------------------------------
    # XCOM
    # This return value is passed through XCom.
    # -----------------------------------------------------

    @task
    def get_files():
        files = [
            "transactions.csv",
            "accounts.csv",
            "customers.csv",
        ]

        logger.info("Files to process: %s", files)

        return files

    # -----------------------------------------------------
    # DYNAMIC TASK MAPPING
    # RETRIES
    # CALLBACKS
    # LOGGING
    # POOL
    # IDEMPOTENCY
    # -----------------------------------------------------

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10),
        on_retry_callback=handle_retry,
        on_failure_callback=handle_failure,
        pool="demo_pool",
    )
    def process_file(file_name):
        OUTPUT_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        context = get_current_context()
        run_id = context["run_id"]

        logger.info(
            "Processing %s for run %s",
            file_name,
            run_id,
        )

        # Demonstrate retry behavior.
        #
        # accounts.csv deliberately fails once per DAG run.
        # A small marker allows the retry to succeed.
        retry_marker = (
            OUTPUT_DIR
            / f"{file_name}_{run_id}.retry"
        )

        if (
            file_name == "accounts.csv"
            and not retry_marker.exists()
        ):
            retry_marker.write_text(
                "First attempt intentionally failed."
            )

            logger.warning(
                "Intentionally failing first attempt for %s",
                file_name,
            )

            raise RuntimeError(
                f"Intentional first-attempt failure for {file_name}"
            )

        # Idempotent output:
        # write_text replaces the existing file rather than appending.
        output_file = OUTPUT_DIR / f"{file_name}.processed"

        output_file.write_text(
            f"{file_name} processed successfully.\n"
        )

        logger.info(
            "Successfully processed %s",
            file_name,
        )

        # Small value passed through XCom.
        return str(output_file)

    # -----------------------------------------------------
    # TRIGGER RULE
    #
    # One branch will always be skipped, so the join cannot
    # require every upstream task to succeed.
    # -----------------------------------------------------

    @task(
        trigger_rule="none_failed_min_one_success"
    )
    def join_paths():
        logger.info(
            "Conditional processing completed successfully."
        )

    # -----------------------------------------------------
    # IDEMPOTENT SUMMARY OUTPUT
    # -----------------------------------------------------

    @task
    def write_summary():
        OUTPUT_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        summary_file = OUTPUT_DIR / "summary.txt"

        summary_file.write_text(
            "Capstone DAG completed successfully.\n"
        )

        logger.info(
            "Summary written to %s",
            summary_file,
        )

    # -----------------------------------------------------
    # DAG FLOW
    # -----------------------------------------------------

    database_check = check_database()

    environment = get_environment()

    branch = choose_path(environment)

    files = get_files()

    skip = skip_processing()

    processed_files = process_file.expand(
        file_name=files
    )

    join = join_paths()

    summary = write_summary()

    wait_for_input >> show_run_info >> database_check
    database_check >> environment
    environment >> branch

    branch >> [files, skip]

    files >> processed_files

    [processed_files, skip] >> join

    join >> summary