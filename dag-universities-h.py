from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator

# instanciamos dag   
with DAG(
    'dag-universities-h',
    description='configuracion de dags para grupo de universidades',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    extract = DummyOperator(task_id='extract') # python operator
    transform = DummyOperator(task_id='transform') # python operator
    load = DummyOperator(task_id='load') # conexion a s3

    extract >> transform >> load