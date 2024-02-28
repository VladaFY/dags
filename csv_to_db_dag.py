"""Подключение необходимых библиотек."""
from datetime import datetime
from typing import Union, List, Dict

# import data_quality as dq
from airflow import AirflowException, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from DDS_dag.csv_to_db_func import csv_to_db



with DAG('csv_to_dds',
         description="Загрузка данных со слоя sources на слой dds",
         schedule_interval="0 0 * * *",
         start_date=datetime(2023, 7, 1),
         catchup=False,
         tags=["etl-process"]) as dag:
        
    
    transfer_data = PythonOperator(
        task_id='csv_to_db', python_callable=csv_to_db)

    #  pre_clean_test_solution_table = PostgresOperator(task_id='pre_clean_test_solution_table',
    #                                    sql='delete from vlada_test.test_solution where 1=1 ',
    #                                    postgres_conn_id='pactg_db'
    #                                    )
    
    
    #  load_from_csv_test_solution = BashOperator(task_id="load_from_csv_test_solution",
    #                           bash_command="python /opt/airflow/dags/scripts_and_files/test_solution.py",
    #                           )
    
     # task_pos = BashOperator(task_id="clean_pos_task",
                              # bash_command="python /opt/airflow/dags/scripts_and_files/cleaning_pos.py",
                              # )

    # remove_all_data >> [brand_upload, category_upload, stores_upload]
    # [brand_upload, category_upload] >> product_upload
    # stores_upload >> [transaction_stores_upload, stock_upload, stores_emails_upload]
    # product_upload >> [product_quantity_upload, stock_upload, transaction_upload]
    # transaction_stores_upload >> transaction_upload        

#     remove_all_data
    #  pre_clean_test_solution_table >> load_from_csv_test_solution