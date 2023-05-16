import pendulum
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["function"]
)

def branch_func():
    @task.branch()
    def condition():
        result = True
        if result:
            return 'success_task_func'
        else:
            return 'failed_task_func'

    @task()
    def success_task_func():
        print('SUCCESS')

    @task()
    def failed_task_func():
        print('FAILED')

    condition() >> [success_task_func(), failed_task_func()]

branch_func()