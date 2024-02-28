import datetime

from airflow import DAG, XComArg
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from DDS_dag.dds_to_dm_func import transfer_data


with DAG(dag_id='source_to_dds_dag',
         schedule_interval='@once',
         start_date=datetime.datetime(2023, 7, 17),
         catchup=False) as dag:

    start_step = DummyOperator(task_id='start_step')

        
    transfer_data_task = PythonOperator(
        task_id='check_table', python_callable=transfer_data, op_kwargs={'target_table': 'final_report'})
    

start_step >> transfer_data_task

