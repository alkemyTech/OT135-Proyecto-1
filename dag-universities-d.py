from lib2to3.pgen2.pgen import DFAState
import logging
import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def extract_process():
    user = 'alkemy_super_user'
    passwd = 'JkG3Ymc3AZuu'
    host = 'training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com'
    port = '5432'
    db = 'training'
    sql = open('sql/universidades-d.sql', 'r').read()
    df = pd.read_sql(f"{sql}", f'postgresql://{user}:{passwd}@{host}:{port}/{db}')
    home = os.getcwd()
    if os.path.exists(f"{home}/file") == False:
        os.makedirs(f"{home}/file")
    return df.to_csv(f"{home}/file/universidades-d.csv")



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
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_process()
    )
    process = DummyOperator(task_id='process')
    load = DummyOperator(task_id='load')

    extract >> process >> load
