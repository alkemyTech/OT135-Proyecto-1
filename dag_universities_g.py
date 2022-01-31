from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

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
    default_args= default_args,
    description = 'Este es un DAG configurado para hacer un ETL para el grupo de universidades G sin consultas ni procesamiento',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 27)
)as dag:
    extract = DummyOperator(task_id='extract') # extract from sql
    transform = DummyOperator(task_id='transform') # transform with pandas
    load = DummyOperator(task_id='load') # load to s3
    extract >> transform >> load