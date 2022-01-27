from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator

with DAG(
    'universidades-F',
    description = 'DAG correspondiente a las universidades de Morón y Río Cuarto',
    schedule_interval = timedelta(hours = 1),
    start_date = datetime(2022,1,28)
) as dag:
    extract = DummyOperator(task_id = 'Extract data from DB')
    transform = DummyOperator(task_id = 'Transform data using Pandas')
    load = DummyOperator(task_id = 'Load data')
    extract >> transform >> load