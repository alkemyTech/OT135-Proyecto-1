import logging
import os
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import exc

logging.basicConfig(level=logging.DEBUG, #previously ERROR
                    format='%(asctime)s - %(module)s - %(message)s',
                    datefmt='%Y-%m-%d'
                    )

DB_USER = config('DB_USER') 
DB_PASSWORD = config('DB_PASSWORD') 
DB_HOST = config('DB_HOST') 
DB_PORT = config('DB_PORT') 
DB_NAME = config('DB_NAME') 

def extract_data():
    DIR = os.path.dirname(__file__)
    PATH_TO_CSV_FILES =  f'{DIR}/files'
    ARCHIVO = f'{DIR}/sql/universidades-c.sql'
    try:
        os.makedirs(PATH_TO_CSV_FILES, exist_ok=True)
    except IOError as e:
        logging.error(f'Error al crear los directorios: {e}')
        raise Exception('Ha ocurrido un error al crear los directorios')
    sql_connection = ('postgresql+psycopg2://'
        + DB_USER
        + ':'
        + DB_PASSWORD
        + '@'
        + DB_HOST
        + '/'
        + DB_NAME
    )
    try:
        with open(ARCHIVO,'r') as sql_file:
            sql_query = sql_file.read()
    except IOError as e:
        logging.error(f'Error al leer los archivos SQL: {e}')
        raise Exception('Ha ocurrido un error al leer los archivos SQL')
    try:
        dataframe = pd.read_sql_query(sql_query,sql_connection)
    except exc.SQLAlchemyError as e:
        logging.error(f'Error al trabajar con la base de datos: {e}')
        raise Exception('Error al trabajar con la base de datos')
    try:
        dataframe.to_csv(f'{PATH_TO_CSV_FILES}/universities_c.csv', encoding='utf-8-sig', index=False)
    except IOError as e:
        logging.error(f'Error al crear los archivos CSV: {e}')
        raise Exception('Ha ocurrido un error al crear los archivos CSV')

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_universities_c',
    default_args=default_args,
    description='DAG  Universidad Nacional De Jujuy - Universidad De Palermo. Doc de los operators a futuro',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    query_sql = PythonOperator(
        task_id = 'query_sql',
        python_callable = extract_data
        )
    pandas_process = DummyOperator(task_id='pandas_process')
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
