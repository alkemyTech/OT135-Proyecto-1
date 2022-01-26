from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators import PythonOperator, BaseSQLOperator


# instanciamos dag   
with DAG(
    'dag-universities-h',
    description='configuracion de dags para grupo de universidades h',
    schedule_interval=timedelta(days=1, hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    extract = BaseSQLOperator(task_id='extract')
    transform = PythonOperator(task_id='transform')
    load = DummyOperator(task_id='load')

    extract >> transform >> load