import logging as log
from datetime import timedelta, datetime
import os

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy
from decouple import config
import boto3


log.basicConfig(
    level=log.ERROR,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
)
logger = log.getLogger('DAG-H')
# Estos argumentos se pasarán a cada operador
# Se pueden anular por tarea durante la inicialización del operador
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
        log.info('Conexión exitosa con la base de datos')
    except Exception as e:
        log.error(e)
        raise e
    # Se lee .sql y se exporta con csv
    try:
        with open(f'{dir}/sql/universidades-h.sql', 'r') as query:
            df = pd.read_sql_query(query.read(), con=engine)
        # Creo carpeta files si no existe
        os.makedirs(f"{dir}/files", exist_ok=True)
        df.to_csv(f'{dir}/files/universidades-h.csv')
        log.info('CSV creado con exito')
    except Exception as e:
        log.error(e)
        raise e

def load_s3(*op_args):
    """
    PythonOperator that uploads a file into a s3 bucket 
    """
    for university in op_args:
        DIR = os.path.dirname(__file__)
        FILE = f'{DIR}/files/{university}'
        logger.info(FILE)
        BUCKET_NAME = config('BUCKET_NAME')
        PUBLIC_KEY = config('PUBLIC_KEY')
        SECRET_KEY = config('SECRET_KEY')
        s3 = boto3.resource('s3', aws_access_key_id=PUBLIC_KEY, aws_secret_access_key=SECRET_KEY)
        try:
            s3.meta.client.upload_file(FILE, BUCKET_NAME, f'{university}')
        except Exception as e:
            logger.error(f'Ocurrió un error: {e}')
            raise e

# instanciamos dag
with DAG(
    'dag-universities-h',
    # diccionario de argumentos predeterminado
    default_args=default_args,
    description='configuracion de dags para grupo de universidades',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    sql_query = PythonOperator(
        task_id='sql_query',
        python_callable=sql_query_extract,
    )
    transform = DummyOperator(task_id='transform')  # python operator
    load = PythonOperator(
        task_id='load',
        python_callable=load_s3,
        op_args=['universidad_del_cine.txt']
    )
    sql_query >> transform >> load
