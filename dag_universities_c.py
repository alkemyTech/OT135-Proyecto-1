import logging
from os import makedirs
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import config

logging.basicConfig(level=logging.DEBUG, #previously ERROR
                    format='%(asctime)s - %(module)s - %(message)s',
                    datefmt='%Y-%m-%d'
                    )

def extract_data():
    PATH_TO_CSV_FILES = 'airflow/dags/files'
    ARCHIVO = 'airflow/dags/sql/universidades-c.sql'
    makedirs(PATH_TO_CSV_FILES, exist_ok=True)
    sql_connection = ('postgresql+psycopg2://'
        + config.DB_USER
        + ':'
        + config.DB_PASSWORD
        + '@'
        + config.DB_HOST
        + '/'
        + config.DB_NAME
    )
    with open(ARCHIVO,'r') as sql_file:
        sql_query = sql_file.read()
    dataframe = pd.read_sql_query(sql_query,sql_connection)
    dataframe.to_csv(PATH_TO_CSV_FILES + '/universities_c.csv')

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
    query_sql = PythonOperator(
        task_id = 'query_sql',
        python_callable = extract_data
        )
    pandas_process = DummyOperator(task_id='pandas_process')
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
