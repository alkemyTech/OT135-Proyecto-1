from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    'dag_universities_b',
    description='DAG  Univ. Nacional Del Comahue - Universidad Del Salvador',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26),
    retries=10,
    retry_delay=timedelta(seconds=1)
    
) as dag:
    query_sql = DummyOperator(task_id='query_sql')
    pandas_process = DummyOperator(task_id='pandas_process')
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
