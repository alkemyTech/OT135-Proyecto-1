import logging

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

logging.basicConfig(
    #filename = './DAG-e.log',
    level = logging.DEBUG,
    filemode = 'w',
    format='%(asctime)s - Universidad Tecnológica Nacional / Universidad Nacional De Tres De Febrero - %(message)s',
    datefmt='%Y-%m-%d',
    )
logger = logging.getLogger()
logger.info('Iniciando DAG')
#logger.error('Mensaje de Error')

with DAG(
    'dag-universities-d',
    description='Configuración de un DAG para el grupo de universidades d',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,1,27)
) as dag:
    extract = DummyOperator(task_id='extract')
    process = DummyOperator(task_id='process')
    load = DummyOperator(task_id='load')

    extract >> process >> load
