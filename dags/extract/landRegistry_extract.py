from airflow.sdk import dag,task
from include.airflow_utils import stream_url_to_s3, DATA_IMAGE
import os
from datetime import datetime, timedelta

config_dict = os.environ.copy()

# default_args = {
#     "depends_on_past": False,
#     "on_failure_callback": slack_failed_task,
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1),
#     "sla": timedelta(hours=12),
#     "sla_miss_callback": slack_failed_task,
#     "start_date": datetime(2023, 5, 10),
#     "dagrun_timeout": timedelta(hours=6),
# }

