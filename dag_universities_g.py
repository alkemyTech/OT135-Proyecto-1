import logging 
import os

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import pandas as pd
from sqlalchemy import create_engine


logging.basicConfig(
		# muestra fecha, nombre de la universidad y error
        level = logging.ERROR,                
        format = '%(asctime)s: %(module)s - %(message)s',
        datefmt = '%Y-%m-%d'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DIR = os.path.dirname(__file__)
PATH_SQL_FILE = f'{DIR}/sql/universidades-g.sql'
PATH_CSV_FILE = f"{DIR}/files/universidades-g.csv"


def sql_query_to_csv(PATH_SQL_FILE, PATH_CSV_FILE):
    """Lee la consulta del archivo sql en formato de DataFrame a través de la conexión realizada
    con la base de datos, y luego lo exporta en formato csv.

    Args:
        PATH_SQL_FILE (string): route to sql file
    """
    
    DB_USER = config("DB_USER")
    DB_PASSWORD = config("DB_PASSWORD")
    DB_HOST = config("DB_HOST")
    DB_PORT = config("DB_PORT")
    DB_NAME = config("DB_NAME")

    # Conexión con base de datos
    try:
        path_connection = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(path_connection, pool_size=1)
    except Exception as e:
        logger.error(e)
        raise e

    try:
        with open(PATH_SQL_FILE, "r") as sql_file:
            df_read = pd.read_sql(sql_file.read(), engine)
        os.makedirs(f"{DIR}/files", exist_ok= True)
        df_read.to_csv(PATH_CSV_FILE)
        logger.info("csv file successfully exported")
    except Exception as e:
        logger.error(e)
        raise e


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
        python_callable=sql_query_to_csv,
        op_args=[PATH_SQL_FILE, PATH_CSV_FILE]
    ) # extract from sql
    transform = DummyOperator(task_id='transform') # transform with pandas
    load = DummyOperator(task_id='load') # load to s3

    extract >> transform >> load