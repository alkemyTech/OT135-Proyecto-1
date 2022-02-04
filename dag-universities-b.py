import logging as log
from datetime import timedelta, datetime
from configparser import ConfigParser
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy

#Establecemos la ruta al directorio local
dir = os.path.dirname(__file__)

#Se configura el formato de logging.ERROR
log.basicConfig(level=log.ERROR,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')

#Se configura la cantidad de reintentos en caso de que el DAG falle
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Se coonfigura la conexión con la base de datos
config = ConfigParser()
config.read(f'{dir}/config.cfg')
cfg = config["DBCONFIG"]
engine = sqlalchemy.create_engine(
    f'postgresql+psycopg2://{cfg["_username"]}:{cfg["_password"]}@{cfg["_databasehost"]}:{cfg["_port"]}/{cfg["_databasename"]}')
log.info('Conexión exitosa con la base de datos')

def sql_query_extract():
    '''
    Lee la SQL query para las universidades B
    Crea un DataFrame con Pandas
    Exporta el df a un archivo csv dentro de la carpeta files
    '''
    try:
        query = open(f'{dir}/sql/universidades-b.sql', 'r')
        df = pd.read_sql_query(query.read(), con=engine)
        # Creo carpeta files si no existe
        if not os.path.exists(f"{dir}/files"):
            os.makedirs(f"{dir}/files")
        df.to_csv(f'{dir}/files/universidades-b.csv')
        query.close()
        log.info('CSV creado con exito')
    except Exception as e:
        log.error(e)

with DAG(
    'dag-universities-b',
    description='Hacer un ETL para la Universidad Nacional del Comahue y Universidad del Salvador',
    schedule_interval='@hourly',
    start_date=datetime(2022, 1, 26)
) as dag:
    sql_query = PythonOperator(
        task_id='sql_query',
        python_callable=sql_query_extract,
    )
    pandas_processing = DummyOperator(task_id='pandas_processing')
    data_load_S3 = DummyOperator(task_id='data_load_S3')

    sql_query >> pandas_processing >> data_load_S3
