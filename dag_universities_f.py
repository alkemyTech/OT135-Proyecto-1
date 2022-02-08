import pandas as pd
import logging
import os
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

# Conexion a la base de datos
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

def extract():
    '''
    Ejecuta una consulta SQL y almacena
    el resultado en la carpeta files con formato csv
    '''
    route = os.path.dirname(__file__)
    file_sql = f'{route}/sql/universidades-f.sql'
    with open(file_sql, 'r') as query:
        sql_query = query.read()
    df = pd.read_sql_query(sql_query, engine)        
    # Pregunto si la carpeta existe y sino la crea 
    if not(os.path.exists(f"{route}/files")):
        os.makedirs(f"{route}/files")        
    df.to_csv(f'{route}/files/universities-f.csv')

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
