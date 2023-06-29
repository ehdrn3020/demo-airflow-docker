import logging
from datetime import datetime

from botocore.config import Config
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task


lambda_job = 'my_lambda_function'
config = Config(
    read_timeout=900,
    retries={
        'max_attempts':1
    }
)

def default_args():
    default_args = {
        'depends_on_past':False,
        'start_date':datetime(2023, 6, 26),
    }
    return default_args

@dag(
    default_args=default_args(),
    schedule_interval='30 2 * * *',
    catchup=False,
    tags=["aws"]
)

def run_lambda(lambda_job, config):
    hook = LambdaHook(
        aws_conn_id='aws_default',
        region_name='ap-northeast-2',
        config=config
    )
    response = hook.invoke_lambda(
        function_name=lambda_job,
        invocation_type='RequestResponse',
        qualifier='$LATEST'
    )
    logging.info(f"response : {response}")

lambda_task_1 = PythonOperator(
    task_id='lambda_run_1_task',
    python_callable=run_lambda,
    provide_context=True,
    op_kwargs={
        'lambda_job':lambda_job,
        "config":config
    }
)

def lambda_task_2():
    @task()
    def lambda_run_2_task():
        hook = LambdaHook(
            aws_conn_id='my_aws_conn',
            region_name='ap-northeast-2',
            config=config
        )

        response = hook.invoke_lambda(
            function_name=lambda_job,
            invocation_type='RequestResponse',
            qualifier='$LATEST'
        )
        logging.info(f"response : {response}")


lambda_task_1 >> lambda_task_2