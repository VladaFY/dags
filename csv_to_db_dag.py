from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow import DAG

from DDS_dag.csv_to_db_func import csv_to_db



with DAG('csv_to_sql',
         description="Загрузка данных со слоя sources на слой dds",
         schedule_interval="0 1 * * *",
         start_date=datetime(2024, 3, 1),
         catchup=False,
         tags=["csv"]) as dag:
        
    start_step = DummyOperator(task_id='start_step')

    end_step = DummyOperator(task_id='end_step')
    
    transfer_data_employees = PythonOperator(
        task_id='transfer_data_employees', python_callable=csv_to_db,op_kwargs={'table': 'test_employees', 'csv': '/opt/airflow/dags/employees.csv'})
    
    transfer_data_emails = PythonOperator(
        task_id='transfer_data_emails', python_callable=csv_to_db,op_kwargs={'table': 'test_employees', 'csv': '/opt/airflow/dags/emails.csv'})
    
    transfer_data_salary = PythonOperator(
        task_id='transfer_data_salary', python_callable=csv_to_db,op_kwargs={'table': 'test_employees', 'csv': '/opt/airflow/dags/salary.csv'})
    

    start_step >> [transfer_data_employees, transfer_data_emails, transfer_data_salary] >> end_step