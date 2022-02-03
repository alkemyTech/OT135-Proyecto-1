from configparser import ConfigParser
import logging 
import os


from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

logging.basicConfig(
		# muestra fecha, nombre de la universidad y error
        level = logging.ERROR,                
        format = '%(asctime)s: %(module)s - %(message)s',
        datefmt = '%Y-%m-%d'
)


# Lee los parámetros de configuración de la base de datos
config = ConfigParser()
config.read('/home/aguyanzon/Alkemy/apache-aiflow-aceleracion/airflow/dags/OT135-Proyecto-1/template.cfg')
cfg = config['DBCONFIG']


# Conexión con base de datos
params = "postgresql+psycopg2://{}:{}@{}/{}".format(
    cfg["DB_USER"], cfg["DB_PASSWORD"],
    cfg["DB_HOST"], cfg["DB_NAME"])
engine = create_engine(params, pool_size=1)


def sql_query_to_csv():
    '''Lee la consulta del archivo sql en formato de DataFrame a través de la conexión realizada
    con la base de datos, y luego lo exporta en formato csv.
    '''
    try:
        sql_file = open('/home/aguyanzon/Alkemy/apache-aiflow-aceleracion/airflow/dags/OT135-Proyecto-1/sql/universidades-g.sql', "r")
        df_read = pd.read_sql(sql_file.read(), engine)
        df_read.to_csv("files/universidades-g.csv")
        logging.info("csv file successfully exported")
    except Exception as error:
        logging.error(error)


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
    extract = PythonOperator(
        task_id='extract',
        python_callable=sql_query_to_csv
    ) # extract from sql
    transform = DummyOperator(task_id='transform') # transform with pandas
    load = DummyOperator(task_id='load') # load to s3

    extract >> transform >> load