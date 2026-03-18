"""Utility DAG used to test Slack failure notifications."""

import logging
import os
from datetime import UTC, datetime
from typing import Never

from airflow.sdk import dag, task

from include.airflow_utils import amber_dags_defaults

SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "airflow_con_slack_data_science_webhook")
logger = logging.getLogger(__name__)



@dag(
    dag_id="slack",
    start_date=datetime(2025, 1, 5, tzinfo=UTC),
    schedule=None,
    catchup=False,
    default_args=amber_dags_defaults,

)
def slack_notifier_dag() -> None:
    """Build the Slack notification test DAG."""

    @task
    def failing_task() -> Never:
        logger.info("This task will fail")
        message = "Simulated failure for Slack alert test"
        raise ValueError(message)

    failing_task()

slack_notifier_dag()


if __name__ == "__main__":
    slack_notifier_dag().test()
