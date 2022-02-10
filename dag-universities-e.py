import pandas as pd
import logging
import csv
import boto3
from sqlalchemy import create_engine, text
from decouple import config
import os

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s: %(module)s: %(message)s", "%Y-%m-%d")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_PORT = config('DB_PORT')

route = os.path.dirname(__file__)

SQL_SCRIPT = f'{route}/sql/universidades-e.sql'

def extract():
    '''Toma la conexion a la base de datos engine y a partir del sql.script
    definido ejecuta sus queries sobre la base de datos y lo guarda como un
    archivo csv'''
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(), echo=False, client_encoding='utf8')
    logger.info('successfully connection')
    logger.info(f'FILE NAME {SQL_SCRIPT}')
    try:
        with open (SQL_SCRIPT, 'r') as query:
           df_query = pd.read_sql_query(query.read(), engine)
           logger.info('sql successfully executed')
           os.makedirs(r'./files', exist_ok=True)
           df_query.to_csv(f'{route}/files/universities-e.csv')
           logger.info('csv successfully save')
    except Exception as ex:
        logger.error(ex)
        raise ex

def load_to_s3():
    '''
    Upload a file to an S3 bucket
    :return: True if file was uploaded, else False
    '''
    route = os.path.dirname(__file__)
    name_txt = 'universidad abierta interamericana.txt'
    file = f'{route}/files/{name_txt}'
    ACCESS_KEY = config('ACCESS_KEY')
    SECRET_KEY = config('SECRET_KEY')
    BUCKET = config('BUCKET')
    # Upload the file
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    s3_client.create_bucket(Bucket=BUCKET)
    try:        
        with open(f'{route}/txt/archivo.txt', 'rb') as f:
            s3_client.upload_fileobj(file, BUCKET, name_txt)
    except ClientError as e:
        logging.error(e)
        return False
    return True

with DAG(
    'dag-universities-e',
    description='Configuración de DAG para el grupo de universidades E',
    default_args={
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 27)
) as dag:
    extract_from_sql = DummyOperator(task_id='extract_from_sql')
    transform_with_pandas = DummyOperator(task_id='transform_with_pandas')
    load_to_s3 = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3
    )

    extract_from_sql >> transform_with_pandas >> load_to_s3
