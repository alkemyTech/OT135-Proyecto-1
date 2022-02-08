import logging as log
from datetime import timedelta, datetime
import os

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy
from decouple import config


log.basicConfig(
    level=log.ERROR,
    format='%(asctime)s - %(module)s - %(message)s',
    datefmt='%Y-%m-%d'
)

# Estos argumentos se pasarán a cada operador
# Se pueden anular por tarea durante la inicialización del operador
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def sql_query_extract():
    '''
    Lee la SQL query para las universidades B
    Crea un DataFrame con Pandas
    Exporta el df a un archivo csv dentro de la carpeta files
    '''
    # Establecemos la ruta al directorio local
    dir = os.path.dirname(__file__)

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
        log.info('Conexión exitosa con la base de datos')
    except Exception as e:
        log.error(e)
        raise e
    # Se lee .sql y se exporta con csv
    try:
        with open(f'{dir}/sql/universidades-h.sql', 'r') as query:
            df = pd.read_sql_query(query.read(), con=engine)
        # Creo carpeta files si no existe
        os.makedirs(f"{dir}/files", exist_ok=True)
        df.to_csv(f'{dir}/files/universidades-h.csv')
        log.info('CSV creado con exito')
    except Exception as e:
        log.error(e)
        raise e

def data_nomalization():
    # Establecemos la ruta al directorio local
    dir = os.path.dirname(__file__)

    #Leemos el archivo .csv con Pandas
    df = pd.read_csv(f'{dir}/files/universidades-h.csv')

    #Elimino la columna unnamed del dataframe
    df = df.drop(['Unnamed: 0'], axis=1)

    #Creo una variable con las columnas no numéricas
    columns = ['university', 'career', 'student_full_name', 'gender', 'location', 'email']

    #Creo una iteración para las colúmnas no numérica
    for column in columns:
        #Convierto el dataframe a minúsculas
        df[column] = df[column].str.lower()
        #Reemplazo en el dataframe los "_" por un espacio
        df[column] = df[column].replace(to_replace='-', value=' ', regex=True)
        #Quitamos los espacios de más
        df[column] = df[column].str.strip()

    #Reemplazo valores de 'm' y 'f' por 'male' y 'female' en la columna gender
    df['gender'] = df['gender'].replace(to_replace='m', value='male', regex=True)
    df['gender'] = df['gender'].replace(to_replace='f', value='female', regex=True)

    #Limpiamos la columna full_name para que nos queden solo los nombres y apellidos
    salutations = ["mrs\.", "mr\.", "dr\.", "ms\.", "md", "dds", "dvm", "iii", "phd", "jr\.", "ii", "iv", "miss"]
    df['student_full_name'] = df['student_full_name'].replace(salutations, value='', regex=True)
    #Separamos nombre y apellido de la columna full_name, los agregamos al df y eliminamos la columna full_name
    name = df_names = df['student_full_name'].str.split(expand=True)
    name.columns = ['first_name', 'last_name']
    df = pd.concat([df, name], axis=1)
    df = df.drop(['student_full_name'], axis=1)

    #Creo una función para calcular la edad
    def age(born):
        born = datetime.strptime(born, "%Y-%m-%d").date() 
        today = date.today() 
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    #Creo una columna con la edad de las personas
    df['age'] = df['birth_date'].apply(age)
    df = df.drop(['birth_date'], axis=1)

    #Leemos el archivo .csv con los codigos postales y las localidades con Pandas
    df_cod_postales = pd.read_csv(f'{dir}/files/codigos_postales.csv')
    #Nos quedamos con solo un código postal por localidad
    df_cod_postales = df_cod_postales.drop_duplicates(['localidad'], keep='first')
    df_cod_postales['localidad'] = df_cod_postales['localidad'].str.lower()
    #Renombramos las columnas para que coincidan con las de df
    df_cod_postales = df_cod_postales.rename(columns={'codigo_postal':'postal_code', 'localidad':'location'})
    # Hago un merge de ambos dataframes para obtener localidades
    merge_location = pd.merge(df, df_cod_postales, how="left", on="postal_code")
    merge_location.location_x = merge_location.location_x.fillna(merge_location.location_y)
    df.location = merge_location.location_x
    # Elimino duplicados de df_codigos_postales
    df_codigos_postales = df_cod_postales.drop_duplicates(['location'], keep='first')
    # Hago otro merge de ambos dataframes para obtener codigos postales
    merge_codes = pd.merge(df, df_codigos_postales, how="left", on="location")
    merge_codes.postal_code_x = merge_codes.postal_code_x.fillna(merge_codes.postal_code_y)
    df.postal_code = merge_codes.postal_code_x
    #Transformamos los valores de la postal_code en int
    df['postal_code'] = df.postal_code.astype(int)

    #Creamos un df filtrando por universidad de buenos aires
    df_universidad_bs_as =df[df['university']=='universidad de buenos aires']
    #Exportamos el df de la universidad de buenos aires como un .txt
    df_universidad_bs_as.to_csv('universidad_de_buenos_aires.txt')
    #Creamos un df filtrando por universidad del cine
    df_universidad_cine =df[df['university']=='universidad del cine']
    #Exportamos el df de la universidad de buenos aires como un .txt
    df_universidad_cine.to_csv('universidad_del_cine.txt')


# instanciamos dag
with DAG(
    'dag-universities-h',
    # diccionario de argumentos predeterminado
    default_args=default_args,
    description='configuracion de dags para grupo de universidades',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    sql_query = PythonOperator(
        task_id='sql_query',
        python_callable=sql_query_extract,
    )
    transform = DummyOperator(task_id='transform')  # python operator
    load = DummyOperator(task_id='load')  # conexion a s3

    sql_query >> transform >> load
