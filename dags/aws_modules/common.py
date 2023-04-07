from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.python import PythonOperator

def aws_default_args(target_email=Variable.get('email_info', deserialize_json=True)):
    default_args = {
        'owner':'dgk',
        'depends_on_past':False,
        'start_date':datetime(2023, 4, 7),
        'email':target_email,
        'email_on_failure':True,
        'email_on_retry':False,
        'retries':1,
        'retry_delay':timedelta(minutes=1)
    }
    return default_args

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

def set_work_date(service_name, execution_date, **context):
    context['ti'].xcom_push(key=f'work_date_{service_name}', value=execution_date)
    print(f"service_name:{service_name}, execution_date:{execution_date}")