from datetime import datetime

def default_args():
    default_args = {
        'owner':'my',
        'depends_on_past':False,
        'start_date':datetime(2023, 5, 15),
    }
    return default_args