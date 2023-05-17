import os
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from func_modules.common import default_args

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(
    DAG_ID,
    default_args=default_args(),
    schedule_interval=None,
    catchup=False,
    tags=['fucntion']
)

start_task = EmptyOperator(
    task_id='start_task',
    queue='researsh',
    dag=dag
)

trigger_task = TriggerDagRunOperator(
    task_id='run_trigger',
    trigger_dag_id='trigger_get',
    execution_date='{{ execution_date }}',
    wait_for_completion=True,
    poke_interval=30,
    conf={
        "message":"start trigger"
    },
    reset_dag_run=True,
    dag=dag
)

start_task >> trigger_task