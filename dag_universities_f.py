import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# basic logging config
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG, datefmt='%I:%M:%S')

with DAG(
    'universidades-F',
    description = 'DAG correspondiente a las universidades de Morón y Río Cuarto',
    schedule_interval = timedelta(hours=1),
    start_date = datetime(2022, 1, 28),
    default_args = {
            'retries': 5,
            'retry_delay': timedelta(minutes=5)
            },
) as dag:
    extract = DummyOperator(task_id = 'extract_data')
    transform = DummyOperator(task_id = 'transform_data')
    load = DummyOperator(task_id = 'load_data')

    extract >> transform >> load
