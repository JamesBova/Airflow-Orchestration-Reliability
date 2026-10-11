import logging
from datetime import datetime, timezone

from airflow.sdk import DAG, get_current_context, task
from airflow.providers.standard.operators.bash import BashOperator


logger = logging.getLogger(__name__)


with DAG(
    dag_id="03_b_jinja_templating",
    description="Deeper look at Airflow Jinja templating and runtime context",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={
        "report_name": "Daily Operations Report",
    },
    tags=["reliability", "deep-dive"],
) as dag:

    show_template_context = BashOperator(
        task_id="show_template_context",
        bash_command="""
        echo "DAG ID: {{ dag.dag_id }}"
        echo "Task ID: {{ task.task_id }}"
        echo "Run ID: {{ run_id }}"
        echo "Logical Date: {{ ds }}"

        echo "Data Interval Start: {{ data_interval_start }}"
        echo "Data Interval End: {{ data_interval_end }}"

        echo "Parameter: {{ params.report_name }}"

        echo "Airflow Variable: {{ var.value.environment_name }}"

        echo "Connection Host: {{ conn.demo_postgres.host }}"
        echo "Connection Database: {{ conn.demo_postgres.schema }}"
        echo "Connection Login: {{ conn.demo_postgres.login }}"
        """,
    )

    @task
    def show_python_context():
        context = get_current_context()

        logger.info(
            "Run ID from Python: %s",
            context["run_id"],
        )

        logger.info(
            "Logical date from Python: %s",
            context["logical_date"],
        )

        logger.info(
            "Data interval start: %s",
            context["data_interval_start"],
        )

        logger.info(
            "Data interval end: %s",
            context["data_interval_end"],
        )

        logger.info(
            "Report name from params: %s",
            context["params"]["report_name"],
        )

    python_context = show_python_context()

    show_template_context >> python_context