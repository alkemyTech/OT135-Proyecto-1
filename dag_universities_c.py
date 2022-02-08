import logging
import os

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(module)s - %(message)s', datefmt='%Y-%m-%d')
logger = logging.getLogger(__name__)

def csv_processing():
    """
    A function to be used as a pyhton operator to read the csv file and process later with pandas
    """
    dir = os.path.dirname(__file__)
    try: 
        df = pd.read_csv(f"{dir}/files/universidades-c.csv")
    except FileNotFoundError:
        logger.error("the file you are trying to access doesn't exist, check the path and the name of the file"  )
    else:
        logger.info(df.head())    

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_universities_c',
    default_args=default_args,
    description='DAG  Universidad Nacional De Jujuy - Universidad De Palermo. Doc de los operators a futuro',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    query_sql = DummyOperator(task_id='query_sql')
    pandas_process = PythonOperator(
        task_id='pandas_process',
        python_callable=csv_processing
        )
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
