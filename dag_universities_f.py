import pandas as pd
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from decouple import config

# basic logging config
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(module)s - %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d')

DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_PORT = config('DB_PORT')

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

'''
    Ejecuta una consulta SQL y almacena
    el resultado en la carpeta files con formato csv
'''
def extract():
    query = open('universidades-f.sql', 'r')
    df = pd.read_sql_query(query.read(), engine)
    query.close()
    df.to_csv('files/universities-f.csv')

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
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )
    transform = DummyOperator(task_id='transform_data')
    load = DummyOperator(task_id='load_data')

    extract >> transform >> load
