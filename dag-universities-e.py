from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag-universities-e',
    description = 'Configuración un DAG, sin consultas, ni procesamiento para el grupo de universidades E',
    default_args = {
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022, 1, 27)
) as dag:
    extract_from_sql = DummyOperator(task_id='extract_from_sql')
    transform_with_pandas = DummyOperator(task_id='transform_with_pandas')
    load_to_s3 = DummyOperator(task_id='load_to_s3')

    extract_from_sql >> transform_with_pandas >> load_to_s3