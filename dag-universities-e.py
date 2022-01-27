from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag-universities-e',
    description = 'Configuración un DAG, sin consultas, ni procesamiento para el grupo de universidades E',
    schedule_interval = timedelta(hours = 1),
    start_date = datetime(2022,1,27)
) as dag:
    extract = DummyOperator(task_id='extract from sql')
    transform = DummyOperator(task_id='transform with pandas')
    load = DummyOperator(task_id='load to s3')

    extract >> transform >> load