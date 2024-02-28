
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd
import csv
import ssl
import smtplib

def get_connect(conn_id):
    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        print("CONNECTION SUCCESS")
        return conn
    except Exception as error:
        raise AirflowException("ERROR: Connect error: {}".format(error))
    
# def csv_to_db():
    # with get_connect('pactg_db').cursor() as cursor:
        # connection.cursor() as cursor
        # with open("/opt/airflow/dags/employees.csv",  'r', errors='ignore') as file:
            # cursor.copy_expert("COPY vlada_test.test_employees FROM stdin WITH DELIMITER as ';'", file)
        # cursor.copy_expert(
            # sql="COPY vlada_test.test_solution FROM stdin WITH DELIMITER as ','",
            # file='/employees.csv'
        # )
            
def csv_to_db():
    conn = get_connect('pactg_db')
    with conn.cursor() as cursor:
        with open("/opt/airflow/dags/employees.csv", 'r') as file:
            # Читаем файл CSV и удаляем нулевые байты
            clean_data = file.read()
            for i in clean_data:
                print(i)
            # Записываем очищенные данные во временный файл
            # with open("/opt/airflow/dags/clean_employees.csv", 'w') as clean_file:
            #     clean_file.write(clean_data)
            
            # Копируем данные из временного файла clean_employees.csv в базу данных
            # with open("/opt/airflow/dags/employees.csv", 'r') as clean_file:
            cursor.copy_expert("COPY vlada_test.test_employees FROM stdin WITH DELIMITER as ','", file)
    conn.commit()
    conn.close()
# def load_data(csv, table):
#     with get_connect('pactg_db') as conn:
#         engine = conn.get_sqlalchemy_engine()
#         with engine.begin() as conn:
#             with 
#                 cursor = conn.cursor()
#                 cmd = 'COPY tbl_name(col1, col2, col3) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
#                 cursor.copy_expert(cmd, f)
#                 conn.commit()

#             # delete old data
#             for table in reversed(tables):
#                 engine.execute(f'truncate table dds.{table} CASCADE;')

#             for final_table, error_table in zip(tables, error_tables):
#                 print(f'START_LOADING {final_table}')

#                 cleared_df = storage[final_table][0]
#                 df_error = storage[final_table][1]

#                 cleared_df.to_sql(final_table, engine, schema='dds',
#                                 if_exists='append', index=False)

#                 df_error.to_sql(error_table, engine, schema='exceptions',
#                                 if_exists='append', index=False)
#                 print(f"LOADING SUCCESS {final_table}")
        