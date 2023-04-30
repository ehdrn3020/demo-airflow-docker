from datetime import datetime, timedelta
from airflow.models import Variable

def aws_default_args(target_email=Variable.get('email_info', deserialize_json=True)):
    default_args = {
        'owner':'my',
        'depends_on_past':False,
        'start_date':datetime(2023, 4, 7),
        'email':target_email,
        'email_on_failure':True,
        'email_on_retry':False,
        'retries':1,
        'retry_delay':timedelta(minutes=1)
    }
    return default_args