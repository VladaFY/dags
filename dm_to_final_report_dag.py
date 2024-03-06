"""Подключение необходимых библиотек."""
from datetime import datetime
from airflow import  DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from DDS_dag.dds_to_final_report_func import load_final_report



with DAG('dds_to_final_report_dag',
         description="Загрузка данных со слоя sources на слой dds",
         schedule_interval="0 3 * * *",
         start_date=datetime(2024, 3, 1),
         catchup=False) as dag:
    
    final_report = PythonOperator(
        task_id='final_report', python_callable=load_final_report
        , op_kwargs={'table': 'final_report'})
    
    
final_report