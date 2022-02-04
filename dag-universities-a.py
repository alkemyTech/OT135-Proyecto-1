import os
import logging
from datetime import timedelta, datetime
from shutil import ExecError
from decouple import config
import pandas as pd
from sqlalchemy import create_engine
from signal import pthread_kill
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(module)s - %(message)s',
                    datefmt='%Y-%m-%d')

DIR = os.path.dirname(__file__)
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_PORT = config('DB_PORT')
QUERY = f"{DIR}/sql/universidades-e.sql"

def query():
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(),
                               echo=False, 
                               client_encoding='utf8')
        query = open(QUERY, 'r')
        df_query = pd.read_sql_query(query.read(), engine)
        df_query.to_csv(f'{DIR}/files/universities-e.csv')
    except:
        logging.error(Exception) # Si ocurre un error no muestra el nombre del grupo de universidades


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Instanciamos dag
with DAG(
    'dag_universities_a',
    default_args=default_args,
    description='DAG para la Universidad De Flores y Universidad Nacional De Villa María. Documenta los operators que se utilizan a futuro, teniendo en cuenta que se va a hacer una consulta SQL, se van a procesar los datos con pandas y se van a cargar los datos en S3',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26),
) as dag:
    query_sql = PythonOperator(task_id='query_sql',
                               python_callable=query) # Consulta SQL
    pandas_process = DummyOperator(task_id='pandas_process') # Procesar datos con pandas
    load_S3 = DummyOperator(task_id='load_S3') # Carga de datos en S3

    query_sql >> pandas_process >> load_S3
