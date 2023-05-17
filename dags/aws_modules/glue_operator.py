import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from aws_modules.common import aws_default_args

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
aws_info = Variable.get('aws_info', deserialize_json=True)
glue_job = 'glue_daily_job'

dag = DAG(
    DAG_ID,
    default_args=aws_default_args(),
    schedule_interval='30 5 * * *',
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
        queue='name_queue',
        dag=dag
    )
    return check_log_task

glue_daily_job_task = GlueJobOperator(
    task_id='glue_daily_job',
    job_name=glue_job,
    aws_conn_id='aws_default',
    queue='name_queue',
    script_args={
        '--target_dt':"{{ ti.xcom_pull(key='work_date') }}"
    },
    s3_bucket='glub-job-bucket',
    iam_role_name='AWSGlueServiceRole-my',
    script_location='s3://glub-job-scm/info/glue/staging_glue_daily_job.py',
    create_job_kwargs={
        "GlueVersion": "2.0",
        "NumberOfWorkers": 30,
        "WorkerType": "G.1X",
        "DefaultArguments": {
            "--class": "GlueApp",
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://glub-job-bucket/glue/sparkui/",
            "--enable-job-insights": "false",
            "--enable-continuous-cloudwatch-log": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
            "--TempDir": "s3://glub-job-bucket/glue/log"
        }
    },
    dag=dag
)

check_work_date_task = check_work_date(dag, 'glue')
check_work_date_task >> glue_daily_job_task