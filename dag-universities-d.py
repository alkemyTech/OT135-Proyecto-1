from lib2to3.pgen2.pgen import DFAState
import logging
import os
import numpy as np
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import pandas as pd

#logger configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d',
    )
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter) 
logger.addHandler(stream_handler)

#database connection
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')

def extract_process():
    """
    Function to be used as a PythonOperator callable funtion to extract the data from the sql file and save it as a CSV 
    """
    home = os.path.dirname(__file__)
    with open(f'{home}/sql/universidades-d.sql', 'r') as sql: 
        try:
            df = pd.read_sql(sql.read(), f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        except FileNotFoundError as ex:    
            logger.error("sorry the file doesn't exist")
	    #raise ex
        else:
            os.makedirs(f"{home}/files", exist_ok = True)
            df.to_csv(f"{home}/files/universidades-d.csv")
            logger.info('csv file created correctly')

#UNIVERSIDADES = 'Universidad Tecnológica Nacional / Universidad Nacional De Tres De Febrero'                      

def filter_universities(df):       
    '''
    Separa el archivo csv en dos archivos .txt para las universidades: 
    Univ. Tecnológica Nacional y Univ. Nacional De Tres De Febrero
    df -- recibe el archivo
    route -- recibe la ruta de donde debe guardar el achivo
    '''
    univer_tecnologica = 'universidad tecnológica nacional'
    univer_tres_febrero = 'universidad nacional de tres de febrero' 

    # Separaro en dos el df, uno por cada universidad
    groups = df.groupby(df.university)
    df_utecnologica = pd.DataFrame(groups.get_group(univer_tecnologica))
    df_utres_febrero = pd.DataFrame(groups.get_group(univer_tres_febrero)) 
    
    # Busco los codigos postales y las localidades faltantes
    route = os.path.dirname(__file__)
    try:
        csv_file = f'{route}/files/codigos_postales.csv'
        df_postal_code = pd.read_csv(f'{csv_file}', encoding='utf-8')
        # Elimino localidades duplicadas
        df_postal_code.drop_duplicates(subset='localidad', inplace = True)      
        # Lo convierto a str para comparar con el df de las universidades
        df_postal_code['codigo_postal'] = df_postal_code['codigo_postal'].astype(str)

        df_utecnologica = df_utecnologica.merge(df_postal_code, 'left', left_on='location', right_on='localidad')        
        df_utres_febrero = df_utres_febrero.merge(df_postal_code, 'left', left_on='postal_code', right_on='codigo_postal')
                                        
    except FileNotFoundError as e:
            logging.error(f'Error al leer el achivo codigo_postales.csv')
            return False     
    
    # Elimino las columnas que no son necesarias (name, birth_date, inscription_date y las que se crearon en el merge) y reordeno
    df_utecnologica['location'] = df['location'].str.lower()
    df_utecnologica.drop(['full_name', 'birth_date', 'postal_code', 'localidad'], axis='columns', inplace=True)
    df_utecnologica.rename(columns={'codigo_postal':'postal_code'}, inplace=True)
    
    df_utres_febrero.drop(['full_name', 'birth_date', 'codigo_postal', 'location'], axis='columns', inplace=True)
    df_utres_febrero.rename(columns={'localidad':'location'}, inplace=True)
    df_utres_febrero['location'] = df['location'].str.lower()

    # Reacomodo las columnas
    new_index = ['university', 'career', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']
    df_utecnologica = df_utecnologica[new_index]
    df_utres_febrero = df_utres_febrero[new_index]

    # Guardo el resultado en dos archivos txt separados por universidades en la carpeta txt
    os.makedirs(f'{route}/txt', exist_ok=True)
    
    df_utecnologica.to_csv(f'{route}/txt/{univer_tecnologica}.txt', index=None, sep='\t', mode='w')    
    df_utres_febrero.to_csv(f'{route}/txt/{univer_tres_febrero}.txt', index=None, sep='\t', mode='w')  

def normalize_info(df):        
    '''
    Normaliza la informacion de los datos y elimina las columnas no solicitadas    
    Datos Finales (columnas): 
    - university, career, first_name, last_name, gender, age, postal_code, location, email
    df -- recibe el archivo 
    '''
    # Año actual para calcular la edad
    today = datetime.today().year
    df['university'] = df['university'].str.lower().str.replace("_", " ").str.strip()
    df['career'] = df['career'].str.lower().str.replace("_", " ").str.strip()
    df['full_name'] = df['full_name'].str.replace("_", " ")
    df['first_name'] = df['full_name'].str.lower().str.split(' ', expand=True)[0].str.strip()
    df['last_name'] = df['full_name'].str.lower().str.split(' ', expand=True)[1].str.strip()
    df['gender'] = df.gender.str.replace("m", "male").str.replace("f", "female")
    df['birth_date'] = df['age'].str.replace("-", " ")
    df['age'] = today - df['birth_date'].str.split(' ', expand=True)[0].astype(int)
    df['postal_code'] = (df['postal_code'].astype(str)).str.replace(".0", "")
    df['location'] = df['location'].str.upper().str.replace("-", " ").str.strip()
    df['email'] = df['email'].str.lower().str.replace("-", " ").str.strip()
    return df

def process():
    route = os.path.dirname(__file__)
    try:
        csv_file = f'{route}/files/universidades-d.csv'
        df = pd.read_csv(f'{csv_file}', index_col=[0], encoding='utf-8')
        # Llamo a la funcion que normaliza la informacion
        normalize_info(df)

        # Llamo a la funcion que separa las universidades y las guarda en archivos .txt
        filter_universities(df)    
    except FileNotFoundError as e:
        logging.error(f'Error al leer el achivo universidades-d.csv')
        return False  

#Se configuran los retries para todo el dag
default_args = {
	'retries': 5,
	'retry_delay': timedelta(minutes=1),
}
with DAG(
    'dag-universities-d',
    description='Configuración de un DAG para el grupo de universidades d',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,1,27)
) as dag:
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_process
    )
    # Eliminar python_callable antes de subir al repo
    process = PythonOperator(task_id='process', python_callable=process)
    load = DummyOperator(task_id='load')

    extract_data >> process >> load
