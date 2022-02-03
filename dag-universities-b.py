import logging as log
from datetime import timedelta, datetime
from configparser import ConfigParser

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy


#Se configura el formato de logging.ERROR
log.basicConfig(level=log.ERROR,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Se coonfigura la conexión con la base de datos
config = ConfigParser()
config.read('config.cfg')
cfg = config["DBCONFIG"]
engine = sqlalchemy.create_engine(
    f'postgresql+psycopg2://{cfg["_username"]}:{cfg["_password"]}@{cfg["_databasehost"]}:{cfg["_port"]}/{cfg["_databasename"]}')
log.info('Successfully connected to DB')

with DAG(
    'dag-universities-b',
    description='Hacer un ETL para la Universidad Nacional del Comahue y Universidad del Salvador',
    schedule_interval='@hourly',
    start_date=datetime(2022, 1, 26)
) as dag:
    def sql_query():
        query = open('sql/universidades-b.sql', 'r')
        df = pd.read_sql_query(query.read(),
                               con=engine)
        df.to_csv('files/universidades-b.csv')
        query.close()

    sql_query = PythonOperator(
        task_id='sql_query',
        python_callable=sql_query,
    )

    pandas_processing = DummyOperator(task_id='pandas_processing')
    data_load_S3 = DummyOperator(task_id='data_load_S3')

    sql_query >> pandas_processing >> data_load_S3
