import logging
from datetime import datetime, timedelta

from airflow.decorators import task, dag

def default_args():
    default_args = {
        'owner': 'my',
        'depends_on_past': False,
        'start_date': datetime(2023, 6, 26)
    }
    return default_args

@dag(
    default_args=default_args(),
    schedule_interval=None,
    catchup=False,
    tags=["function"]
)

def get_last_saturday(dt):
    d = dt.toordinal()
    last = d - 6
    sunday = last - (last % 7)
    saturday = sunday + 6
    return datetime.fromordinal(saturday).strftime("%Y%m%d")

def reusing_task():
    @task()
    def set_date(num_day, **kwargs):
        logical_date = kwargs["logical_date"].in_timezone('Asia/Seoul')
        last_saturday = get_last_saturday(logical_date)
        work_date = (datetime.strptime(last_saturday, '%Y%m%d') - timedelta(days=num_day)).strftime('%Y%m%d')
        logging.info(f'work_date:{work_date}')
        return work_date

    # 인자 값에 따라 다른 값을 return하는 각각의 task 정의
    get_last_sat_dt_task = set_date(0)
    get_last_fri_dt_task = set_date.override(task_id="get_last_fri_dt")(1)

    get_last_sat_dt_task >> get_last_fri_dt_task

reusing_task()