import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from aws_modules.common import aws_default_args

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
aws_info = Variable.get('aws_info', deserialize_json=True)

dag = DAG(
    DAG_ID,
    default_args=aws_default_args(),
    schedule_interval=None,
    catchup=False,
    tags=['aws']
)

def set_work_date(service_name, execution_date, **context):
    context['ti'].xcom_push(key=f'work_date', value=execution_date)
    print(f"service_name:{service_name}, execution_date:{execution_date}")

def check_work_date(dag, service_name):
    check_log_task = PythonOperator(
        task_id='check_work_date',
        python_callable=set_work_date,
        op_kwargs={
            "service_name": service_name,
            "execution_date":"{{execution_date.in_timezone('Asia/Seoul').strftime('%Y%m%d')}}"
        },
        queue='research',
        dag=dag
    )
    return check_log_task

select_table_task = AthenaOperator(
    task_id=f'select_table',
    query=f"select * from {aws_info['table']} where dt='{{{{ti.xcom_pull(key=\"work_dt\")}}}}' limit 20",
    database=aws_info['db'],
    workgroup='primary',
    aws_conn_id='aws_default',
    queue='research',
    output_location=aws_info['output_location'],
    dag=dag
)

check_work_date_task = check_work_date(dag, 'athena')
check_work_date_task >> select_table_task