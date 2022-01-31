import logging

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

logging.basicConfig(level=logging.ERROR, format='%Y-%m-%d:%(name)s:%(message)s')

with DAG(
    'dag_universities_c',
    description='DAG  Universidad Nacional De Jujuy - Universidad De Palermo. Doc de los operators a futuro',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    query_sql = DummyOperator(task_id='query_sql')
    pandas_process = DummyOperator(task_id='pandas_process')
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
