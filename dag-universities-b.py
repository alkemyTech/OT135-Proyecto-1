from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag-universities-b',
    description= 'Hacer un ETL para la Universidad Nacional del Comahue y Universidad del Salvador',
    schedule_interval=timedelta(days=1), #'@hourly'
    start_date= datetime(2022, 1, 25)
) as dag:
    sql_query = DummyOperator(task_id='sql_query')
    pandas_processing = DummyOperator(task_id='pandas_processing')
    data_load_S3 = DummyOperator(task_id='data_load_S3')

    sql_query >> pandas_processing >> data_load_S3

