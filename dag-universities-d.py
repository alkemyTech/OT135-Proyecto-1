import logging
import os
from datetime import timedelta, datetime

import pandas as pd
from decouple import config
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from lib2to3.pgen2.pgen import DFAState
import boto3

# logger configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d',
    )
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter) 
logger.addHandler(stream_handler)

# database connection
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')

def extract_process():
    """
    Function to be used as a PythonOperator callable funtion to extract the data from the sql file and save it as a CSV 
    """
    home = os.path.dirname(__file__)
    with open(f'{home}/sql/universidades-d.sql', 'r') as sql: 
        try:
            df = pd.read_sql(sql.read(), f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        except FileNotFoundError as ex:    
            logger.error("sorry the file doesn't exist")
            raise ex # Me tiraba error de indentación
        else:
            os.makedirs(f"{home}/files", exist_ok = True)
            df.to_csv(f"{home}/files/universidades-d.csv")
            logger.info('csv file created correctly')

# UNIVERSIDADES = 'Universidad Tecnológica Nacional / Universidad Nacional De Tres De Febrero'

def load_s3():
    """
    PythonOperator that uploads a file into a s3 bucket 
    """
    DIR = os.path.dirname(__file__)
    FILE = f'{DIR}/files/universidad_de_tres_de_febrero.txt'
    logger.info(FILE)
    BUCKET_NAME = config('BUCKET_NAME')
    PUBLIC_KEY = config('PUBLIC_KEY')
    SECRET_KEY = config('SECRET_KEY')
    s3 = boto3.resource('s3', aws_access_key_id=PUBLIC_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        s3.meta.client.upload_file(FILE, BUCKET_NAME, 'universidad_de_tres_de_febrero.txt')
    except Exception as e:
        logger.error(f'Ocurrió un error: {e}')
        raise e

# Se configuran los retries para todo el dag
default_args = {
	'retries': 5,
	'retry_delay': timedelta(minutes=1),
}
with DAG(
    'dag-universities-d',
    description='Configuración de un DAG para el grupo de universidades d',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 27)
) as dag:
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_process
    )
    process = DummyOperator(task_id='process')
    load = PythonOperator(
        task_id='load',
        python_callable=load_s3
    )

    extract_data >> process >> load
