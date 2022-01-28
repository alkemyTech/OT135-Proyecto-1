from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator

with DAG(
    'dag-universidades-g',
    description = 'Este es un DAG configurado para hacer un ETL para el grupo de universidades G sin consultas ni procesamiento',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 27)
)as dag:
    extract = DummyOperator(task_id='extract_from_sql')
    transform = DummyOperator(task_id='transform_with_pandas')
    load = DummyOperator(task_id='load_to_s3')
    extract >> transform >> load