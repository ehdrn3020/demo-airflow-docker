import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow import AirflowException

from aws_modules.common import aws_default_args

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
aws_info = Variable.get('aws_info', deserialize_json=True)

dag = DAG(
    DAG_ID,
    default_args=aws_default_args(),
    catchup=False,
    tags=['aws']
)

def list_return():
    return ['hellow', 'name', 'world']

fetch_package_task = PythonOperator(
    task_id='fetch_package',
    python_callable=list_return,
    queue='name_queue',
    dag=dag
)

def athena_query(database, output_location, **context):
    package_list = context['task_instance'].xcom_pull(task_ids='fetch_package')
    if len(package_list) > 0 :
        athena_hook = AthenaHook(aws_conn_id='aws_default')
        package_str = ','.join([f"'{elem[0]}'" for elem in package_list])
        query = f'SELECT package, order FROM package_list_order WHERE package in ({package_str})'

        query_execution_id = athena_hook.run_query(
            query=query,
            query_context=dict(Database=database),
            result_configuration=dict(OutputLocation=output_location)
        )
        print(f"ATHENA QUERY ID:{query_execution_id}")
        result_status = athena_hook.poll_query_status(query_execution_id, 10)
        if result_status != "SUCCEEDED":
            err_msg = athena_hook.get_state_change_reason(query_execution_id)
            print(f"ATHENA QUERY NOT SUCCESS:{result_status}")
            print(f"CAUSE:{err_msg}")
            raise AirflowException("athena query failed")
        query_result = athena_hook.get_query_results(query_execution_id=query_execution_id)

        athena_package_list = [line['Data'][0]['VarCharValue'] for i, line in enumerate(query_result['ResultSet']['Rows']) if i != 0]
        athena_order_list = [line['Data'][1]['VarCharValue'] for i, line in enumerate(query_result['ResultSet']['Rows']) if i != 0]
        print(f"athena_package_list:{athena_package_list}")
        print(f"athena_order_list:{athena_order_list}")
        return athena_package_list, athena_order_list

athena_fetch_package_task = PythonOperator(
    task_id='athena_fetch_package',
    python_callable=athena_query,
    op_kwargs={
        "database":aws_info['db'],
        "output_location":aws_info['output_location']
    },
    provide_context=True,
    queue='name_queue',
    dag=dag
)

fetch_package_task >> athena_fetch_package_task