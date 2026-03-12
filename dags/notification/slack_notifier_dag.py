"""Utility DAG used to test Slack failure notifications."""

import logging
import os
from datetime import datetime

from airflow.decorators import dag, task

from include.airflow_utils import amber_dags_defaults

SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "airflow_con_slack_data_science_webhook")
logger = logging.getLogger(__name__)



# def slack_successful_task(context):
#     """slack_successful_task Function to be used as a callable for on_success_callback

#     Args:
#         context (_type_): _description_
#     """
#     attachment, slack_channel, task_id, task_text = slack_defaults(context, "success")
#     airflow_http_con_id, slack_webhook = slack_webhook_conn(slack_channel)
@dag(
    dag_id="slack",
    start_date=datetime(2025, 1, 5),
    schedule=None,
    catchup=False,
    default_args=amber_dags_defaults,

)
def slack_notifier_dag():

    @task
    def failing_task():
        logger.info("This task will fail")
        raise ValueError("Simulated failure for Slack alert test")

    failing_task()

slack_notifier_dag()


if __name__ == "__main__":
    slack_notifier_dag().test()
