import logging
import os

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import boto3
from botocore.exceptions import ClientError


logging.basicConfig(
    # muestra fecha, nombre de la universidad y error
    level=logging.ERROR,
    format='%(asctime)s: %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
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
        PATH_SQL_FILE (string): ruta al archivo sql
        PATH_CSV_FILE (string): ruta al archivo csv

    Raises:
        e: si existe error en la conexión con la base de datos
        e: si existe error en la lectura del archivo .sql o en la exportación del archivo .csv
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
        os.makedirs(f"{DIR}/files", exist_ok=True)
        df_read.to_csv(PATH_CSV_FILE)
        logger.info("csv file successfully exported")
    except Exception as e:
        logger.error(e)
        raise e


def load():
    BUCKET_NAME = config('BUCKET_NAME')
    PUBLIC_KEY = config('PUBLIC_KEY')
    SECRET_KEY = config('SECRET_KEY')
    s3 = boto3.resource('s3', aws_access_key_id=PUBLIC_KEY, aws_secret_access_key=SECRET_KEY)
    s3.meta.client.upload_file(f'{DIR}/txt/facultad_latinoamericana_de_ciencias_sociales.txt', BUCKET_NAME, 'facultad_latinoamericana_de_ciencias_sociales.txt')
    

def transform():
    '''
    Esta función transforma los datos previamante extraidos mediante una consulta sql 
    a las columnas requeridas y posteriormente genera un archivo .txt con los datos
    para cada una de las universidades en el grupo g
    '''
    try:
        df = pd.read_csv(PATH_CSV_FILE, index_col=False)
        dfaux = pd.read_csv(f'{DIR}/codigos_postales.csv')
    except Exception as e:
        logger.error('Hubo un error en la lectura de un archivo csv')
        raise e
    
    df['university'] = df['university'].str.lower().str.replace('-',' ')
    df['university'] = df['university'].str.lstrip(' ')
    df['career'] = df['career'].str.lower().str.replace('-',' ')
    df['career'] = df['career'].str.rstrip(' ').str.lstrip(' ')
    WORDS = ['dr.','mr.','mrs.-','hill','md','phd','ii','iii','md','dvm','phd','dds','iv','ms.']
    df.full_name.replace(WORDS,'', regex=True, inplace=True)
    df['full_name'] = df['full_name'].str.lstrip('.')
    df['full_name'] = df['full_name'].str.lstrip('-')
    df['first_name'] = df['full_name'].str.lower().str.split('-', expand=True)[0]
    df['last_name'] = df['full_name'].str.lower().str.split('-', expand=True)[1]
    df['gender'] = df['gender'].str.lower().str.replace('m','male').str.replace('f','female')
    df['age'] = (int(datetime.today().year) - pd.DatetimeIndex(df['birth_date']).year).astype(int)
    df['email'] = df['email'].str.lower()
    df['adress'] = df['adress'].str.replace('-', ' ')
    df['zipcode'] = df['zipcode'].astype(str)
    df['zipcode'] = df['zipcode'].str[:4]

    # A continuación, se mergean las columnas de zipcode y adress con sus correspondientes en la tabla auxiliar para
    # obtener los datos que faltan y luego juntar todos en las dos columnas postal_code y location
    
    mrge1 = pd.merge(
        df[['zipcode','adress']].astype(str),
        dfaux.astype(str),
        how="left",
        left_on='zipcode',
        right_on='codigo_postal',
    )
    mrge2 = pd.merge(
        df[['zipcode','adress']].astype(str),
        dfaux.drop_duplicates(subset=['localidad']).astype(str),
        how="left",
        left_on='adress',
        right_on='localidad'
    )
    df['postal_code'] = mrge1['zipcode'].replace('nan','') + mrge2['codigo_postal'].replace(np.NaN,'')
    df['location'] = (mrge1['localidad'].replace(np.NaN,'') + mrge2['localidad'].replace(np.NaN,'')).str.lower()

    df.drop(['full_name', 'birth_date', 'inscription_date','adress','zipcode'], axis='columns', inplace=True)
    df = df.reindex(columns=['university', 'career', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email'])

    # Se filtran los datos y se crea un dataframe para cada universidad, para posteriomente guardarse
    # en la carpeta txt

    df_kennedy = df[df['university']=='universidad j. f. kennedy']
    df_latinoamericana = df[df['university']=='facultad latinoamericana de ciencias sociales']

    try:
        os.makedirs(f'{DIR}/txt', exist_ok= True)
        df_kennedy.to_csv(f'{DIR}/txt/universidad-j-f-kennedy.txt', index=None)
        df_latinoamericana.to_csv(f'{DIR}/txt/facultad_latinoamericana_de_ciencias_sociales.txt', index=None)
    except Exception as e:
        logger.error('Hubo un error guardando los archivos .txt')
        raise e

def load_to_s3():
    # Establecemos la ruta al archivo de la Universidad JFK
    DIR = os.path.dirname(__file__)
    universidad_txt = f'{DIR}/files/universidad_jf_kennedy.txt'
    # Parametros para la conexión con S3
    BUCKET_NAME = config("BUCKET_NAME")
    PUBLIC_KEY = config("PUBLIC_KEY")
    SECRET_KEY = config("SECRET_KEY")
    s3 = boto3.resource(
        service_name='s3',
        aws_access_key_id=PUBLIC_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    try:
        s3.meta.client.upload_file(
            universidad_txt, BUCKET_NAME, 'univerdidad_jf_kennedy2.txt')
    except ClientError as e:
        logging.error(e)
        return False
    return True



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
    default_args=default_args,
    description='Este es un DAG configurado para hacer un ETL para el grupo de universidades G sin consultas ni procesamiento',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 27)
)as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=sql_query_to_csv,
        op_args=[PATH_SQL_FILE, PATH_CSV_FILE]
    ) # extract from sql
    transform = DummyOperator(task_id='transform') # transform with pandas
    load_1 = PythonOperator(
        task_id='load_1',
        python_callable=load
    ) # load to s3
    load_2 = PythonOperator(
        task_id='load_2',
        python_callable=load_to_s3
    )

    extract >> transform >> [load_1, load_2]
