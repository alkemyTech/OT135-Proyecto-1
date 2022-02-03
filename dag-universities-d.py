from lib2to3.pgen2.pgen import DFAState
import logging
import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import pandas as pd

def extract_process():
    """
    Function to be used as a PythonOperator callable funtion to extract the data from the sql file and save it as a CSV 
    """
    sql = open('sql/universidades-d.sql', 'r').read()
    df = pd.read_sql(f"{sql}", f'postgresql://{user}:{passwd}@{host}:{port}/{db}')
    home = os.path.dirname(__file__)
    try:
        if os.path.exists(f"{home}/file") == False:
            os.makedirs(f"{home}/file")
        df.to_csv(f"{home}/file/universidades-d.csv")
        logging.info('csv file created correctly')
    except Exception as e:
        logging.error(e)


#database connection
user = config('DB_USER')
passwd = config('DB_PASSWORD')
host = config('DB_HOST')
port = config('DB_PORT')
db = config('PSQL_DB')


#UNIVERSIDADES = 'Universidad Tecnológica Nacional / Universidad Nacional De Tres De Febrero'
logging.basicConfig(
    level = logging.DEBUG,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d',
    )
#logger = logging.getLogger()
#logger.error('Iniciando DAG / Mensaje de error')

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
    load = DummyOperator(task_id='load')

    extract_data >> process >> load
