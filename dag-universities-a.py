from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag_universities_a',
    description='DAG para la Universidad De Flores y Universidad Nacional De Villa María. Documenta los operators que se utilizan a futuro, teniendo en cuenta que se va a hacer una consulta SQL, se van a procesar los datos con pandas y se van a cargar los datos en S3',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26),
) as dag:
    query_sql = DummyOperator(task_id='query_sql')
    pandas_process = DummyOperator(task_id='pandas_process')
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
