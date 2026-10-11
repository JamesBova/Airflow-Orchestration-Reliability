import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, task


logger = logging.getLogger(__name__)


with DAG(
    dag_id="05_b_branching",
    description="Deeper look at Airflow branching",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    @task
    def get_processing_mode():
        # Change this to:
        # "full"
        # "transactions_only"
        # "skip"
        return "full"

    @task.branch
    def choose_path(mode):

        if mode == "full":
            return [
                "process_transactions",
                "process_accounts",
            ]

        if mode == "transactions_only":
            return "process_transactions"

        return "skip_processing"

    @task
    def process_transactions():
        logger.info("Processing transactions")

    @task
    def process_accounts():
        logger.info("Processing accounts")

    @task
    def skip_processing():
        logger.info("Processing was skipped")

    @task(
        trigger_rule="none_failed_min_one_success"
    )
    def finish():
        logger.info("Selected processing path completed")

    mode = get_processing_mode()

    branch = choose_path(mode)

    transactions = process_transactions()
    accounts = process_accounts()
    skip = skip_processing()

    end = finish()

    branch >> [
        transactions,
        accounts,
        skip,
    ]

    [
        transactions,
        accounts,
        skip,
    ] >> end