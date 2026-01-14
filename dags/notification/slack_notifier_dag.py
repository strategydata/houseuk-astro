from airflow.decorators import dag, task
from datetime import datetime
import logging
from airflow.models import Variable
import os
from airflow.providers.slack.operators.slack_webhook import SlackWebhookHook
import urllib.parse
from include.airflow_utils import amber_defaults

SLACK_WEBHOOK_CONN_ID = os.environ.get("SLACK_WEBHOOK_CONN_ID", "airflow_con_slack_data_science_webhook")



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
    default_args=amber_defaults,
)
def slack_notifier_dag():

    @task
    def failing_task():
        print("This task will fail 🚨")
        raise ValueError("Simulated failure for Slack alert test")

    failing_task()

slack_notifier_dag()


if __name__ == "__main__":
    slack_notifier_dag().test()