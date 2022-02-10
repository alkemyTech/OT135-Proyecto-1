import logging as log
from datetime import timedelta, datetime
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import sqlalchemy
from decouple import config

# Se configura el formato de logging.ERROR
log.basicConfig(level=log.INFO,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')

logger = log.getLogger(__name__)

# Se configura la cantidad de reintentos en caso de que el DAG falle
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def sql_query_extract():
    '''
    Lee la SQL query para las universidades B
    Crea un DataFrame con Pandas
    Exporta el df a un archivo csv dentro de la carpeta files
    '''
    # Establecemos la ruta al directorio local
    dir = os.path.dirname(__file__)

    # Parámetros de la base de datos
    DB_USER = config("DB_USER")
    DB_PASSWORD = config("DB_PASSWORD")
    DB_HOST = config("DB_HOST")
    DB_PORT = config("DB_PORT")
    DB_NAME = config("DB_NAME")

    # Se coonfigura la conexión con la base de datos
    try:
        engine = sqlalchemy.create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        logger.info('Conexión exitosa con la base de datos')
    except Exception as e:
        logger.error(e)
        raise e
    #Se lee .sql y se exporta con csv
    try:
        with open(f'{dir}/sql/universidades-b.sql', 'r') as query:
            df = pd.read_sql_query(query.read(), con=engine)
        # Creo carpeta files si no existe
        os.makedirs(f"{dir}/files", exist_ok=True)
        df.to_csv(f'{dir}/files/universidades-b.csv')
        logger.info('CSV creado con exito')
    except Exception as e:
        logger.error(e)
        raise e



DIR = os.path.dirname(__file__)
PATH_FILE_TXT = f'{DIR}/files/universidad-del-salvador.txt'

def upload_file_s3(PATH, key):
    """Recibe un archivo .txt y lo sube a un bucket de S3

    Args:
        PATH (string): ruta en donde se encuentra el archivo
        key (string): nombre con el que va a figurar el archivo en S3
    """

    BUCKET_NAME= config('BUCKET_NAME')
    PUBLIC_KEY= config('PUBLIC_KEY')
    SECRET_KEY= config('SECRET_KEY')

    try:
        s3 = boto3.resource(
            's3',
            aws_access_key_id=PUBLIC_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        logger.info('Successful connection')

        with open(PATH, 'rb') as f:
            s3.Bucket(BUCKET_NAME).put_object(
                Key= key,
                Body= f
            )    
        logger.info('File uploaded successfully')
    except Exception as ex:
        logger.error(ex)
        raise ex


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
    data_load_S3 = PythonOperator(
        task_id='data_load_S3',
        python_callable=upload_file_s3,
        op_args=[PATH_FILE_TXT, 'universidad-del-salvador.txt']
    )

    sql_query >> pandas_processing >> data_load_S3
