

if __name__ =="__main__":
    from dags.notification import slack_notifier_dag
    slack_notifier_dag().test()
