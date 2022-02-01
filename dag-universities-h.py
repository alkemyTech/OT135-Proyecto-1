from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator

# Estos argumentos se pasarán a cada operador
# Se pueden anular por tarea durante la inicialización del operador
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# instanciamos dag   
with DAG(
    'dag-universities-h',
    # diccionario de argumentos predeterminado
    default_args=default_args,    
    description='configuracion de dags para grupo de universidades',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 26)
) as dag:
    extract = DummyOperator(task_id='extract') # python operator
    transform = DummyOperator(task_id='transform') # python operator
    load = DummyOperator(task_id='load') # conexion a s3

    extract >> transform >> load