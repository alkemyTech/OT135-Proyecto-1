from lib2to3.pgen2.pgen import DFAState
import logging
import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import pandas as pd
import boto3
from botocore.exceptions import ClientError

#logger configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d',
    )
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter) 
logger.addHandler(stream_handler)

#database connection
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
	    raise ex
        else:
            os.makedirs(f"{home}/files", exist_ok = True)
            df.to_csv(f"{home}/files/universidades-d.csv")
            logger.info('csv file created correctly')

#UNIVERSIDADES = 'Universidad Tecnológica Nacional / Universidad Nacional De Tres De Febrero'

def load_to_s3():
    # Establecemos la ruta al archivo de la UTN
    DIR = os.path.dirname(__file__)
    universidad_txt= f'{DIR}/files/universidad_tecnologica_nacional.txt'
    # Parametros para la conexión con S3
    BUCKET_NAME = config("BUCKET_NAME")
    PUBLIC_KEY = config("PUBLIC_KEY")
    SECRET_KEY = config("SECRET_KEY")

    s3 = boto3.resource('s3', aws_access_key_id=PUBLIC_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        s3.Bucket(BUCKET_NAME).upload_file(universidad_txt, SECRET_KEY)
    except ClientError as e:
        logging.error(e)
        return False
    return True


#Se configuran los retries para todo el dag
default_args = {
	'retries': 5,
	'retry_delay': timedelta(minutes=1),
}
with DAG(
    'dag-universities-d',
    description='Configuración de un DAG para el grupo de universidades d',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,1,27)
) as dag:
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_process
    )
    process = DummyOperator(task_id='process')
    load = PythonOperator(
        task_id='load',
        python_callable=load_to_s3
    )

    extract_data >> process >> load
