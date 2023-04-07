import os
from airflow import DAG
from aws_modules.common import aws_default_args, check_work_date

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(
    DAG_ID,
    default_args=aws_default_args(),
    catchup=False,
    tags=['aws']
)

check_work_date_task = check_work_date(dag, 'athena')

check_work_date_task

