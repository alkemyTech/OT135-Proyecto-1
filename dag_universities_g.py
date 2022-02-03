from configparser import ConfigParser
import logging 

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
		# muestra fecha, nombre de la universidad y error
        level = logging.ERROR,                
        format = '%(asctime)s: %(module)s - %(message)s',
        datefmt = '%Y-%m-%d'
)

# Lee los parámetros de configuración de la base de datos
config = ConfigParser()
config.read('template.cfg')
cfg = config["DBCONFIG"]

# Conexión con base de datos
params = "postgresql+psycopg2://{}:{}@{}/{}".format(
    cfg["DB_USER"], cfg["DB_PASSWORD"],
    cfg["DB_HOST"], cfg["DB_NAME"])
engine = create_engine(params, pool_size=1)


default_args = {
   # 'owner': 'airflow',
   # 'depends_on_past': False,
   # 'email': ['airflow@example.com'],
   # 'email_on_failure': False,
   # 'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag-universidades-g',
    default_args= default_args,
    description = 'Este es un DAG configurado para hacer un ETL para el grupo de universidades G sin consultas ni procesamiento',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 27)
)as dag:
    extract = DummyOperator(task_id='extract') # extract from sql
    transform = DummyOperator(task_id='transform') # transform with pandas
    load = DummyOperator(task_id='load') # load to s3
    extract >> transform >> load