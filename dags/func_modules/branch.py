import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task

def default_args():
    default_args = {
        'owner':'my',
        'depends_on_past':False,
        'start_date':datetime(2023, 5, 15),
    }
    return default_args

@dag(
    default_args=default_args(),
    schedule_interval='30 9 * * *',
    catchup=False,
    tags=["function"]
)

def check_request_exection():
    @task()
    def set_date(**kwargs):
        work_date = kwargs["logical_date"].in_timezone('Asia/Seoul').strftime('%Y%m%d')
        logging.info(f'work_date:{work_date}')
        return work_date

    @task()
    def get_status(work_date:str):
        url=f'https://your_request_url?region=seoul&date={work_date}'
        headers = {
            'Content-Type':'application/json',
            'API-Key':'123456abcdefg'
        }
        result = requests.get(url, headers=headers)
        response = result.json()
        logging.info(f'resonse:{response}')
        result = response.get('result', None)
        return result

    @task.branch()
    def condition(status):
        if status == 'SUCCESS':
            return 'success_status_task'
        else:
            return 'failed_status_task'

    @task()
    def success_status_task():
        logging.info('SUCCESS')

    @task()
    def failed_status_task():
        logging.info('FAILED')

    set_date_task = set_date()
    get_status_task = get_status(set_date_task)
    condition(get_status_task) >> [success_status_task(), failed_status_task()]

check_request_exection()