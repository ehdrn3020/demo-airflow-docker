import os
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(
    DAG_ID,
    start_date=datetime(2023, 4, 7),
    catchup=False,
    tags=['mysql']
)

def mysql_query(query, action, **context):
    # Create a MySqlHook to connect to the MySQL database
    mysql_hook = MySqlHook(mysql_conn_id='mysql_info', schema='db_name')
    query_result = []
    # Fetch data from the MySQL database using the MySqlHook
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    if action == 'select':
        cursor.execute(query)
        query_result = cursor.fetchall()
    elif action == 'update':
        package_list = context['task_instance'].xcom_pull(task_ids='mysql_fetch_package')
        if len(package_list) > 0 :
            for package in package_list:
                package_id = package[0]
                query_str = query.format(package_id)
                print('query_str:',query_str)
                cursor.execute(query_str)
                conn.commit()
                time.sleep(0.5)
        else:
            print(f"package_list is None")
    cursor.close()
    conn.close()
    return query_result

mysql_fetch_package_task = PythonOperator(
    task_id='mysql_fetch_package',
    python_callable=mysql_query,
    op_kwargs={
        "query":"SELECT package_id FROM package_list_order WHERE withdrawl = 'n'",
        "action":"select"
    },
    queue='research',
    dag=dag
)

mysql_update_package_task = PythonOperator(
    task_id='mysql_update_package',
    python_callable=mysql_query,
    op_kwargs={
        "query":"UPDATE package_list_order SET paid = 'y' WHERE package_id = '{}'",
        "action":"update"
    },
    queue='research',
    dag=dag
)

mysql_fetch_package_task >> mysql_update_package_task