import logging as log
from datetime import timedelta, datetime
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import sqlalchemy
from decouple import config

# Se configura el formato de logging.ERROR
log.basicConfig(level=log.INFO,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')

logger = log.getLogger(__name__)

# Se configura la cantidad de reintentos en caso de que el DAG falle
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Establecemos la ruta al directorio local
dir = os.path.dirname(__file__)

def sql_query_extract():
    '''
    Lee la SQL query para las universidades B
    Crea un DataFrame con Pandas
    Exporta el df a un archivo csv dentro de la carpeta files
    '''
    # Parámetros de la base de datos
    DB_USER = config("DB_USER")
    DB_PASSWORD = config("DB_PASSWORD")
    DB_HOST = config("DB_HOST")
    DB_PORT = config("DB_PORT")
    DB_NAME = config("DB_NAME")

    # Se coonfigura la conexión con la base de datos
    try:
        engine = sqlalchemy.create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        logger.info('Conexión exitosa con la base de datos')
    except Exception as e:
        logger.error(e)
        raise e
    #Se lee .sql y se exporta con csv
    try:
        with open(f'{dir}/sql/universidades-b.sql', 'r') as query:
            df = pd.read_sql_query(query.read(), con=engine)
        # Creo carpeta files si no existe
        os.makedirs(f"{dir}/files", exist_ok=True)
        df.to_csv(f'{dir}/files/universidades-b.csv')
        logger.info('CSV creado con exito')
    except Exception as e:
        logger.error(e)
        raise e

def pandas_function():
    """
    Function to be used as a PythonOperator to process the data information from the csv generated with sql_query_extract
    """
    try:
        df=pd.read_csv(f'{dir}/files/universidades-b.csv')
        #setting zipcodes and locations as a DataFrame
        cp=pd.read_csv(f"{dir}/files/codigos_postales.csv")
    except FileNotFoundError:
        log.error("the path or the file you selected is incorrect please check and try again")
    else:
        df=df.drop("Unnamed: 0",axis=1)
        log.info("DataFrame has been created")
        cp.rename(columns = {"codigo_postal": "postal_code", "localidad":"location"} ,inplace = True)
        log.info("postal codes and location dataframe has been loaded")
    #making strings lowercase
    df["university"] = df["university"].str.lower()
    df["career"] = df["career"].str.lower()
    df["full_name"] = df["full_name"].str.lower()
    df["gender"] = df["gender"].str.lower()
    df["location"] = df["location"].str.lower()
    df["email"] = df["email"].str.lower()
    cp["location"] = cp["location"].str.lower()
    #setting gender in the requested format
    df['gender'] = df['gender'].str.replace('m', 'male')
    df['gender'] = df['gender'].str.replace('f', 'female')
    #calculating age
    df['date_of_birth'] = df['date_of_birth'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
    now = pd.Timestamp('now')
    df['age'] = (now - df['date_of_birth']).astype('<m8[Y]')
    #eliminating '_' and salutations from name
    for column in list(df):
        df[f"{column}"] = df[f"{column}"].replace(['_',],' ', regex=True)
    df["full_name"] = df["full_name"].replace(['mrs\. ', 'mr\. ', 'ms\. ', 'dr\. ', 'miss ', 'mister '],'', regex=True)
    #splitting first and last name
    df['first_name'] = df['full_name'].str.split(' ').str[0]
    df['last_name'] = df['full_name'].str.split(' ').str[1]
    #merging location by zipcode
    df = df.merge(cp, on='postal_code', how='left')
    df['location'] = df['location_y'].fillna(df['location_x'])
    df = df.drop(['location_y', 'location_x'], axis=1)
    #merging zipcode by location
    cp.drop_duplicates(subset='location', inplace = True)
    df = df.merge(cp, on='location', how='left')
    df['postal_code'] = df['postal_code_y'].fillna(df['postal_code_x'])
    df = df.drop(['postal_code_y', 'postal_code_x'], axis=1)
    #reordering columns and creating .txt file
    df = df.reindex(columns=['university','career','first_name','last_name','gender','age','postal_code','location'])
    #creating the txt files 
    salvador =df.loc[df['university']=='universidad del salvador']
    salvador.to_csv('data/universidad_del_salvador.txt',index=False)
    comahue =df.loc[df['university']=='univ. nacional del comahue']
    comahue.to_csv('data/univ._nacional_del_comahue.txt',index=False)


DIR = os.path.dirname(__file__)
PATH_FILE_TXT = f'{DIR}/files/universidad-del-salvador.txt'

def upload_file_s3(PATH, key):
    """Recibe un archivo .txt y lo sube a un bucket de S3

    Args:
        PATH (string): ruta en donde se encuentra el archivo
        key (string): nombre con el que va a figurar el archivo en S3
    """

    BUCKET_NAME= config('BUCKET_NAME')
    PUBLIC_KEY= config('PUBLIC_KEY')
    SECRET_KEY= config('SECRET_KEY')

    try:
        s3 = boto3.resource(
            's3',
            aws_access_key_id=PUBLIC_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        logger.info('Successful connection')

        with open(PATH, 'rb') as f:
            s3.Bucket(BUCKET_NAME).put_object(
                Key= key,
                Body= f
            )    
        logger.info('File uploaded successfully')
    except Exception as ex:
        logger.error(ex)
        raise ex


with DAG(
    'dag-universities-b',
    description='Hacer un ETL para la Universidad Nacional del Comahue y Universidad del Salvador',
    schedule_interval='@hourly',
    start_date=datetime(2022, 1, 26)
) as dag:
    sql_query = PythonOperator(
        task_id='sql_query',
        python_callable=sql_query_extract
    )
    pandas_processing = PythonOperator(
        task_id = 'pandas_processing',
        python_callable = pandas_process
        )
    data_load_S3 = PythonOperator(
        task_id='data_load_S3',
        python_callable=upload_file_s3,
        op_args=[PATH_FILE_TXT, 'universidad-del-salvador.txt']
    )

    sql_query >> pandas_processing >> data_load_S3
