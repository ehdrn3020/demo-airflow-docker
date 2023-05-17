import os
from airflow import DAG
import logging
import time
from func_modules.common import default_args
from airflow.operators.python import PythonOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(
    DAG_ID,
    default_args=default_args(),
    schedule_interval=None,
    catchup=False,
    tags=['fucntion']
)

def dump():
    logging.info(f'dump function sleep')
    time.sleep(5)

trigger_start = PythonOperator(
    task_id='trigger_start',
    python_callable=dump,
    dag=dag
)

trigger_task_1 = PythonOperator(
    task_id='trigger_task_1',
    python_callable=dump,
    dag=dag
)

trigger_end = PythonOperator(
    task_id='trigger_end',
    python_callable=dump,
    dag=dag
)

trigger_start >> trigger_task_1 >> trigger_end
