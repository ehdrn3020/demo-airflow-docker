import logging
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime
from airflow.decorators import dag, task
from airflow.decorators import task_group

BUCKET = 'my-bucket'

def default_args():
    default_args = {
        'depends_on_past':False,
        'start_date':datetime(2023, 6, 26),
    }
    return default_args

@dag(
    default_args=default_args(),
    schedule_interval='30 0 * * *',
    catchup=False,
    tags=["aws"]
)

def check_file_upload():
    @task()
    def set_date(**kwargs):
        work_date = kwargs["logical_date"].in_timezone('Asia/Seoul').strftime('%Y%m%d')
        logging.info(f'work_date:{work_date}')
        return work_date

    @task_group
    def trigger_s3_sensor(work_date:str):
        s3_sensor = S3KeySensor(
            task_id='s3_sensor_task',
            aws_conn_id='my_aws_conn',
            poke_interval=10,
            timeout=60 * 60 * 6,
            bucket_key=f"s3://{BUCKET}/input-data/dt={work_date}/_TRIGGER_FILE",
            wildcard_match=False
        )
        s3_sensor

    @task()
    def success_status_task():
        logging.info('SUCCESS')

    set_date_task = set_date()
    trigger_s3_sensor_task = trigger_s3_sensor(set_date_task)
    set_date_task >> trigger_s3_sensor_task >> success_status_task()


check_file_upload()