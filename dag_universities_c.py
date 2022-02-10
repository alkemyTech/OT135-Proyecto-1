import logging
import numpy as np
import os
import pandas as pd
import re
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime, date
from decouple import config
from sqlalchemy import exc

logging.basicConfig(level=logging.DEBUG, #previously ERROR
                    format='%(asctime)s - %(module)s - %(message)s',
                    datefmt='%Y-%m-%d'
                    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_USER = config('DB_USER') 
DB_PASSWORD = config('DB_PASSWORD') 
DB_HOST = config('DB_HOST') 
DB_PORT = config('DB_PORT') 
DB_NAME = config('DB_NAME') 

def extract_data():
    DIR = os.path.dirname(__file__)
    PATH_TO_CSV_FILES =  f'{DIR}/files'
    ARCHIVO = f'{DIR}/sql/universidades-c.sql'
    try:
        os.makedirs(PATH_TO_CSV_FILES, exist_ok=True)
    except IOError as e:
        logging.error(f'Error al crear los directorios: {e}')
        raise Exception('Ha ocurrido un error al crear los directorios')
    sql_connection = ('postgresql+psycopg2://'
        + DB_USER
        + ':'
        + DB_PASSWORD
        + '@'
        + DB_HOST
        + '/'
        + DB_NAME
    )
    try:
        with open(ARCHIVO,'r') as sql_file:
            sql_query = sql_file.read()
    except IOError as e:
        logging.error(f'Error al leer los archivos SQL: {e}')
        raise Exception('Ha ocurrido un error al leer los archivos SQL')
    try:
        dataframe = pd.read_sql_query(sql_query,sql_connection)
    except exc.SQLAlchemyError as e:
        logging.error(f'Error al trabajar con la base de datos: {e}')
        raise Exception('Error al trabajar con la base de datos')
    try:
        dataframe.to_csv(f'{PATH_TO_CSV_FILES}/universities_c.csv', encoding='utf-8-sig', index=False)
    except IOError as e:
        logging.error(f'Error al crear los archivos CSV: {e}')
        raise Exception('Ha ocurrido un error al crear los archivos CSV')

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def age(born):
    '''Esta función toma la columna date y devuelve la edad del estudiante'''
    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    return today.year - born.year - ((today.month, 
                                      today.day) < (born.month, 
                                                    born.day))

def transform():
    '''Esta funcion toma el csv extraido de la base de datos,
    realiza un pre-procesamiento de los datos y devuelve un txt
    por cada grupo de universidades.'''
    
    DIR = os.path.dirname(__file__)

    try:
        df = pd.read_csv(f'{DIR}/files/universities_c.csv')        
        df_zip = pd.read_csv(f'{DIR}/files/codigos_postales.csv')
        logger.info('csv files uploaded successfully')
    except Exception as ex:
        logger.error(ex)
        raise ex
    

    # Crea una variable con las columnas.
    columns = ['university', 'career', 'full_name', 'gender', 'postal_code', 'location', 'email', 'birth_date']
    
    # Reemplaza los simbolos por espacios
    df = df[columns].replace('\_',' ', regex=True)
    logger.info('special character replaced successfully')

    # Reemplazo los valores de la columna gender
    df["gender"] = df["gender"].replace({"m": "male", "f": "female"})
    logger.info('gender replaced successfully')

    # Reemplazo mayusculas por minusculas
    str_columns = ['university', 'career', 'full_name', 'gender', 'location', 'email']

    for c in str_columns:
        df[c] = df[c].str.lower()
        df[c] = df[c].str.strip()

    logger.info('lowered string successfully')
    
    # Separo el nombre y creo dos columnas: first name y last name
    df['first_Name'] = df['full_name'].str.split(' ', expand = True)[0]
    df['last_Name'] = df['full_name'].str.split(' ', expand = True)[1]
    logger.info('split column successfully')

    # Creo la columna age
    df['age'] = df['birth_date'].apply(age)
    logger.info('age column added successfully')
    
    # Elimino las columnas que no se van a usar
    df = df.drop(["full_name", "birth_date"], axis=1)
    
    # Convierto el codigo postal a integer
    df['postal_code'] = df['postal_code'].replace(np.nan, 0)
    df['postal_code'] = df['postal_code'].astype(int) 
    
    # Separo el dataframe y creo dos nuevos teniendo en cuenta la universidad.
    df_jujuy = df[df['university'] == 'universidad nacional de jujuy']
    df_palermo = df[df['university'] == 'universidad de palermo']
    
    # Creo un dataframe a partir del csv codigos_postales y hago un pre-procesamiento
    df_zip = df_zip.rename(columns={'codigo_postal': 'postal_code', 'localidad': 'location'})
    df_zip['location'] = df_zip['location'].str.lower()
    logger.info('rename successfully')

    # Realizo merge de zip code y location
    df_univ_jujuy = pd.merge(df_jujuy, df_zip, how="left", on="location")
    df_univ_palermo = pd.merge(df_palermo, df_zip, how="left", on="postal_code")
    
    df_univ_jujuy = df_univ_jujuy.drop(["postal_code_x"], axis=1)
    df_univ_jujuy = df_univ_jujuy.rename(columns={'postal_code_y': 'zip_code'})
    df_univ_palermo = df_univ_palermo.drop(["location_x"], axis=1)
    df_univ_palermo = df_univ_palermo.rename(columns={'location_y': 'location'})
    
    # GUARDAR EL DF COMO TXT
    try:
        os.makedirs(f'files', exist_ok= True)
        df_univ_jujuy.to_csv(f'{DIR}/files/universidad_nacional_jujuy.txt', index=False)      
        df_univ_palermo.to_csv(f'{DIR}/files/universidad_palermo.txt', index=False)
        logger.info('txt files saved successfully')
    except Exception as ex:
        logger.error(ex)
        raise ex

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
    pandas_process = DummyOperator(
        task_id='pandas_process')
    load_S3 = DummyOperator(task_id='load_S3')

    query_sql >> pandas_process >> load_S3
