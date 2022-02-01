import logging

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s: %(module)s: %(message)s","%Y-%m-%d")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter) 

logger.addHandler(stream_handler)


with DAG(
    'dag-universities-e',
    description='Configuración un DAG, sin consultas, ni procesamiento para el grupo de universidades E',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 27)
) as dag:
    
    extract_from_sql = DummyOperator(task_id='extract_from_sql')
    transform_with_pandas = DummyOperator(task_id='transform_with_pandas')
    load_to_s3 = DummyOperator(task_id='load_to_s3')

    extract_from_sql >> transform_with_pandas >> load_to_s3
