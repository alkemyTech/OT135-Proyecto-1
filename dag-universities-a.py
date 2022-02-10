import os
import logging
import pandas as pd
from datetime import timedelta, datetime, date
from decouple import config
from sqlalchemy import create_engine
from signal import pthread_kill

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(module)s - %(message)s',
                    datefmt='%Y-%m-%d')

logger = logging.getLogger('DAG-A')


def query():
    try:
        DIR = os.path.dirname(__file__)
        DB_USER = config('DB_USER')
        DB_PASSWORD = config('DB_PASSWORD')
        DB_HOST = config('DB_HOST')
        DB_NAME = config('DB_NAME')
        DB_PORT = config('DB_PORT')
        QUERY = f"{DIR}/sql/universidades-a.sql"
        with open(QUERY, 'r') as query:
            engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
                                   echo=False,
                                   client_encoding='utf8')
            df_query = pd.read_sql_query(query.read(), engine)
            os.makedirs(f'{DIR}/files', exist_ok=True)
            df_query.to_csv(f'{DIR}/files/universities-a.csv')
    except Exception as e:
        logger.error(
            'Hubo un error en la consulta sql en las tablas de la  Universidad De Flores y/o la Universidad Nacional De Villa María')
        raise e


def age(birth_date):
    """
    Crea un dataframe normalizado de las locaciones culturales presentes en los archivos CSV
    Inputs:
        Fecha de nacimiento
    Ouput
        Cantidad de años cumplidos
    """
    today = date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))


def clean_words(column):
    """
    Toma una columna de un dataframe y le saca guiones, espacios y demás
    Inputs:
        Columna de un dataframe
    Ouput
        Columna normalizada
    """
    column = column.str.lower()
    column = column.str.replace('mr.', '')
    column = column.str.replace('ms.', '')
    column = column.str.replace(' dds', '')
    column = column.str.replace('mrs.', '')
    column = column.str.replace(' md', '')
    column = column.str.replace('dr.', '')
    column = column.str.replace('_', ' ')
    column = column.str.rstrip()
    column = column.str.lstrip()
    column = column.str.replace('-', '')
    column = column.str.replace('  ', ' ')
    return column


def convert(dataframe, dict_to_location, dict_from_location):
    """
    Dado un dataframe, completa los datos faltantes en códigos postales y localidades
    Inputs:
        - Dataframe, diccionario de códigos a localidades y diccionario de localidades a códigos
    Ouput
        -
    """
    # Uso de fillna para tomar todos los casos posibles de None, NaN, null y ''
    dataframe.location = dataframe.location.fillna(
        dataframe.postal_code.map(dict_to_location))
    # Normalizamos la localidad para poder leer sin problemas del diccionario
    dataframe['location'] = dataframe['location'].str.upper()
    dataframe['location'] = dataframe['location'].str.replace('_', ' ')
    dataframe['location'] = dataframe['location'].str.rstrip()
    dataframe['location'] = dataframe['location'].str.lstrip()
    dataframe.postal_code = dataframe.postal_code.fillna(
        dataframe.location.map(dict_from_location))
    dataframe['location'] = dataframe['location'].str.lower()
    dataframe[['first_name', 'last_name']
              ] = dataframe['full_name'].str.split(' ', 1, expand=True)
    # Drop las columnas no solicitadas
    dataframe.drop('full_name', axis=1, inplace=True)
    dataframe.drop('birth_date', axis=1, inplace=True)


def pandas_process_func():
    """
    Toma los datos de las universidades del CSV y las normaliza a un txt
    Inputs:
        -
    Ouput
        -
    """

    # Cargamos los archivos CSV necesarios
    DIR = os.path.dirname(__file__)
    ARCHIVO_CSV = f'{DIR}/files/universities-a.csv'
    ARCHIVO_VILLAMARIA = f'{DIR}/files/universidad_nacional_de_villa_maria.txt'
    ARCHIVO_FLORES = f'{DIR}/files/universidad_de_flores.txt'
    CODIGOS_POSTALES = f'{DIR}/files/codigos_postales.csv'

    # Leemos el datagrame indicando los tipos de datoas y parseos que hacen falta
    try:
        dataframe = pd.read_csv(ARCHIVO_CSV, parse_dates=['birth_date'], dtype={
                                'postal_code': str}, index_col=False)
    except IOError as e:
        logging.error(f'Error al leer el csv, no se lo ha encontrado: {e}')
        raise Exception('No se encontró el archivo csv')
    # Creamos dos diccionarios para conocer las localidades y códigos postales
    postal_code_to_location = pd.read_csv(
        CODIGOS_POSTALES, header=None, index_col=0).squeeze().to_dict()
    location_to_postal_code = pd.read_csv(
        CODIGOS_POSTALES, header=None, index_col=1).squeeze().to_dict()

    # Limpiamos los strings de espacios, guiones, prefijos y demás
    dataframe['university'] = clean_words(dataframe['university'])
    dataframe['career'] = clean_words(dataframe['career'])
    dataframe['full_name'] = clean_words(dataframe['full_name'])
    dataframe['email'] = clean_words(dataframe['email'])

    # Convertimos el género al formato pedido
    dataframe['gender'] = dataframe['gender'].str.replace('m', 'male')
    dataframe['gender'] = dataframe['gender'].str.replace('f', 'female')

    # Convertimos la fecha de nacimiento a edad
    dataframe['age'] = dataframe['birth_date'].apply(age)
    # Obtenemos localidades y códigos postales
    convert(dataframe, postal_code_to_location, location_to_postal_code)
    # Sacamos el índice del txt final
    dataframe.reset_index(drop=True, inplace=True)
    dataframe_villamaria = dataframe[dataframe['university'].str.contains(
        'universidad nacional de villa maría')]
    dataframe_flores = dataframe[dataframe['university'].str.contains(
        'universidad de flores')]
    try:
        dataframe_villamaria.to_csv(
            ARCHIVO_VILLAMARIA, encoding='utf-8-sig', index=False)
        dataframe_flores.to_csv(
            ARCHIVO_FLORES, encoding='utf-8-sig', index=False)
    except IOError as e:
        logger.error(f'Error al crear el archivo TXT: {e}')
        raise e


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Instanciamos dag
with DAG(
    'dag_universities_a',
    default_args=default_args,
    description='DAG para la Universidad De Flores y Universidad Nacional De Villa María. Documenta los operators que se utilizan a futuro, teniendo en cuenta que se va a hacer una consulta SQL, se van a procesar los datos con pandas y se van a cargar los datos en S3',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26),
) as dag:
    query_sql = PythonOperator(task_id='query_sql',
                               python_callable=query)  # Consulta SQL
    pandas_process = PythonOperator(task_id='pandas_process',
                                    python_callable=pandas_process_func)    # Procesar datos con pandas
    load_S3 = DummyOperator(task_id='load_S3')  # Carga de datos en S3

    query_sql >> pandas_process >> load_S3
