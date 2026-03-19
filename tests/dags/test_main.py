if __name__ == "__main__":
    from dags.notification.slack_notifier_dag import slack_notifier_dag

    slack_notifier_dag().test()
