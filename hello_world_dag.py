from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from sentry_sdk.integrations.serverless import serverless_function
from data_pipeline import logger


@serverless_function
def print_hello():
    logger.info('This is hello world DAG')
    return 'Hello world!'


dag = DAG(
    'hello_world',
    description='Simple tutorial DAG',
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
