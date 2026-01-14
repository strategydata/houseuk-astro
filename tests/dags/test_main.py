import os
import pytest
from airflow.models import DagBag


if __name__ =="__main__":
    import dags.notification.slack_notifier_dag as slack_notifier_dag
    slack_notifier_dag().test()