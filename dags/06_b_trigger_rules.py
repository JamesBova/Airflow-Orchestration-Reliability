import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, task


logger = logging.getLogger(__name__)


with DAG(
    dag_id="06_b_trigger_rules",
    description="Deeper look at Airflow trigger rules",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    @task.branch
    def choose_path():
        return "process_a"

    @task
    def process_a():
        logger.info("Processing path A")

    @task
    def process_b():
        logger.info("Processing path B")

    # Default behavior:
    # every upstream task must succeed.
    @task(
        trigger_rule="all_success"
    )
    def join_all_success():
        logger.info("ALL_SUCCESS join ran")

    # No upstream tasks may fail.
    # Skipped tasks are acceptable.
    @task(
        trigger_rule="none_failed"
    )
    def join_none_failed():
        logger.info("NONE_FAILED join ran")

    # No failures, and at least one upstream task
    # must actually succeed.
    @task(
        trigger_rule="none_failed_min_one_success"
    )
    def join_branch_safe():
        logger.info(
            "NONE_FAILED_MIN_ONE_SUCCESS join ran"
        )

    # Run once every upstream task has reached
    # a terminal state, regardless of success,
    # failure, or skip.
    @task(
        trigger_rule="all_done"
    )
    def join_all_done():
        logger.info("ALL_DONE join ran")

    branch = choose_path()

    a = process_a()
    b = process_b()

    all_success = join_all_success()
    none_failed = join_none_failed()
    branch_safe = join_branch_safe()
    all_done = join_all_done()

    branch >> [a, b]

    [a, b] >> all_success
    [a, b] >> none_failed
    [a, b] >> branch_safe
    [a, b] >> all_done