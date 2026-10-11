import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, task


logger = logging.getLogger(__name__)


with DAG(
    dag_id="07_b_dynamic_task_mapping",
    description="Deeper look at dynamic task mapping",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["reliability", "deep-dive"],
) as dag:

    @task
    def get_file_jobs():
        return [
            {
                "file_name": "transactions.csv",
                "file_type": "transactions",
            },
            {
                "file_name": "accounts.csv",
                "file_type": "accounts",
            },
            {
                "file_name": "customers.csv",
                "file_type": "customers",
            },
        ]

    @task
    def process_file(
        file_name,
        file_type,
        environment,
    ):
        logger.info(
            "Processing %s as type %s in environment %s",
            file_name,
            file_type,
            environment,
        )

        return f"{file_name} complete"

    @task
    def summarize(results):
        logger.info("Mapped processing complete")

        for result in results:
            logger.info(result)

    jobs = get_file_jobs()

    #.expand()...original file
    #→ map individual argument values

    #.expand_kwargs()
    #→ map dictionaries containing multiple arguments

    #.partial()
    #→ provide fixed arguments shared by every mapped instance

    processed = process_file.partial(
        environment="development"
    ).expand_kwargs(jobs)

    summarize(processed)