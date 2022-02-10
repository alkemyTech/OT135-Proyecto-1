import csv
from datetime import timedelta, datetime, date
from dateutil.relativedelta import relativedelta
import logging
import os
import re

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s: %(module)s: %(message)s", "%Y-%m-%d")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')
DB_PORT = config('DB_PORT')

route = os.path.dirname(__file__)

SQL_SCRIPT = f'{route}/sql/universidades-e.sql'

def extract():
    '''Toma la conexion a la base de datos engine y a partir del sql.script
    definido ejecuta sus queries sobre la base de datos y lo guarda como un
    archivo csv'''
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(), echo=False, client_encoding='utf8')
    logger.info('successfully connection')
    logger.info(f'FILE NAME {SQL_SCRIPT}')
    try:
        with open (SQL_SCRIPT, 'r') as query:
           df_query = pd.read_sql_query(query.read(), engine)
           logger.info('sql successfully executed')
           os.makedirs(r'./files', exist_ok=True)
           df_query.to_csv(f'{route}/files/universities-e.csv')
           logger.info('csv successfully save')
    except Exception as ex:
        logger.error(ex)
        raise ex

def data_normalization():
    """
    Lee dos archivos .csv, uno de universidades y otro de códigos postales.
    Los tranforma en dataframes y luego realiza un procesamiento en sus datos para que queden 
    normalizados. Realiza un merge de ambos para completar datos nulos y luego exporta dos archivos 
    de universidades en formato .txt
    """
    try:
        df = pd.read_csv(f'{route}/files/universities-e.csv', parse_dates=['birth_date'])        
        df_codigos_postales = pd.read_csv(f'{route}/files/codigos_postales.csv')
        logger.info('csv files uploaded successfully')
    except Exception as ex:
        logger.error(ex)
        raise ex
    # Tratamiento de df
    df.drop(['Unnamed: 0'], axis=1, inplace=True)
    
    for column in df.columns:
        if (column == 'birth_date') or (column == 'postal_code'):
            df[column] = df[column]
        else:
            df[column] = df[column].str.lower()
        
    # Convierto postal_code a string y elimino .0 del flotante
    df['postal_code'] = df['postal_code'].astype(str)
    df.postal_code = df.postal_code.apply(lambda x: re.sub("\.0","",x))
    
    df.gender = df.gender.map({
        'm' : 'male', 
        'f' : 'female'
    })
    
    # Reemplazo guiones por espacio en las columnas dentro de features
    features = ['university', 'career', 'full_name', 'location']
    for feature in features:
        df[feature] = df[feature].astype(str)
        df[feature] = df[feature].apply(lambda x: re.sub('\-'," ",x))
        
    # diccionario de palabras a eliminar de la columna full_name
    words = {
        'dr.' : '(dr\.)', 
        'mr.' : '(mr\.)', 
        'mrs.': '(mrs\.)', 
        'hill': '(^hi$)', 
        'md'  : '(^md$)', 
        'phd' : '(^phd$)', 
        'ii'  : '(^ii$)', 
        'iii' : '(^iii$)',
        'dvm' : '(^dvm$)',
        'phd' : '(^phd$)', 
        'dds' : '(^dds$)', 
        'iv'  : '(^iv$)'
    }
    # Reemplazo las palabras del diccionario words por espacios vacíos
    for key, value in words.items():
        df.full_name = df.full_name.apply(lambda key: re.sub(value,"",key))
        
    for column in df.columns:
        if column != 'birth_date':
            df[column] = df[column].str.strip()
            
    df['first_name'] = df['full_name'].str.split(' ', expand=True)[0]
    df['last_name'] = df['full_name'].str.split(' ', expand=True)[1]
    df.drop(['full_name'], axis=1, inplace= True)
    
    df['age'] = df.birth_date.apply(lambda birth: (relativedelta(datetime.now(), birth).years))
    df.drop(['birth_date'], axis=1, inplace= True)
    
    df.postal_code.replace('(nan)',np.nan, regex=True, inplace=True)
    df.location.replace('(nan)',np.nan, regex=True, inplace=True)
    
    # Tratamiento de df_codigos_postales
    df_codigos_postales.localidad = df_codigos_postales.localidad.str.lower()
    df_codigos_postales.codigo_postal = df_codigos_postales.codigo_postal.astype(str)
    df_codigos_postales.columns = 'postal_code location'.split()
    # Realizo un merge de ambos dataframes para obtener las localidades y codigos postales faltantes
    df = pd.merge(df, df_codigos_postales, how="left", on="postal_code")
    df['location'] = df.location_y.fillna(df.location_x)

    df.drop(['location_y', 'location_x'], axis=1, inplace=True)

    df_codigos_postales = df_codigos_postales.drop_duplicates(['location'], keep='first')
    df = pd.merge(df, df_codigos_postales, how="left", on="location")
    df['postal_code'] = df.postal_code_y.fillna(df.postal_code_x)
    df.drop(['postal_code_y', 'postal_code_x'], axis=1, inplace=True)
    
    # Reorganizo las columnas del dataframe final
    df = df[[
        'university', 'career', 'first_name', 'last_name', 'gender', 'age', 
        'postal_code', 'location', 'email'
    ]]
    
    df_uni_nac_pampa = df[df.university == 'universidad nacional de la pampa']
    df_uni_abierta_int = df[df.university == 'universidad abierta interamericana']
    
    # Exporto el dataframe final como un archivo .txt
    try:
        os.makedirs(f'files', exist_ok= True)
        df_uni_nac_pampa.to_csv(f'{route}/files/universidad_nacional_pampa.txt', index=False)      
        df_uni_abierta_int.to_csv(f'{route}/files/universidad_abierta_interamericana.txt', index=False)
        logger.info('txt files exported successfully')
    except Exception as exr:
        logger.error(ex)
        raise ex


with DAG(
    'dag-universities-e',
    description='Configuración de DAG para el grupo de universidades E',
    default_args={
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 27)
) as dag:
    extract_from_sql = PythonOperator(
        task_id='extract_from_sql',
        python_callable=extract
    )
    transform_with_pandas = DummyOperator(task_id='transform_with_pandas')
    load_to_s3 = DummyOperator(task_id='load_to_s3')

    extract_from_sql >> transform_with_pandas >> load_to_s3
