import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from aws_modules.common import aws_default_args

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
aws_info = Variable.get('aws_info', deserialize_json=True)

dag = DAG(
    DAG_ID,
    default_args=aws_default_args(),
    schedule_interval='30 5 * * *',
    catchup=False,
    tags=['aws']
)

ARN = 'your_dms_arm'
dms_json = './mappings.json'

# dms table mapping파이을 변경하는 Operator
change_aws_dms_task = BashOperator(
    task_id='change_aws_dms_task',
    bash_command=f'aws dms modify-replication-config --replication-config-arn {ARN} --table-mappings \'{dms_json}\'',
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
)
# dms를 실행하는 Operator
run_aws_dms_task = BashOperator(
    task_id='run_aws_dms_task',
    bash_command=f'aws dms start-replication --replication-config-arn {ARN} --start-replication-type resume-processing'
)
# dms 실행상태를 체크하는 Operator
check_aws_dms_task = BashOperator(
    task_id='check_aws_dms_task',
    bash_command=f'aws dms describe-replications --filter "Name=replication-config-arn,Values=${ARN}" --query "Replications[0]" --output json'
)

change_aws_dms_task >> run_aws_dms_task >> check_aws_dms_task
