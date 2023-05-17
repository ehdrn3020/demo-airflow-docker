from airflow.decorators import task, dag
from datetime import datetime

@task
def add_task(x,y):
    print(f"Task args: x={x}, y={y}")
    return x + y

@dag(start_date=datetime(2022, 1, 1), catchup=False, tags=["function"])
def taskflow_reused_1():
    start = add_task.override(task_id="start")(1, 2)
    for i in range(3):
        start >> add_task.override(task_id=f"add_stat_{i}")(start, i)

@dag(start_date=datetime(2022, 1, 1), catchup=False, tags=["function"])
def taskflow_reused_2():
    start = add_task(1, 2)
    for i in range(3):
        start >> add_task.override(task_id=f"new_add_task_{i}")(start, i)

first_dag = taskflow_reused_1()
second_dag = taskflow_reused_2()