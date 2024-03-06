from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd

from funcs.csv_to_db_func import get_connect

def get_data():
    with get_connect('pactg_db') as conn:
        print('START_EXTRACT')
        query = """
            SELECT emp.ID as Empl_ID, 
                emp.NAME1 || ' ' || emp.NAME2 || ' ' || emp.NAME3 as FIO,
                AVG(CASE WHEN sal.Salary_Type = 'salary' THEN sal.Amount ELSE NULL END) as Salary,
                AVG(CASE WHEN sal.Salary_Type = 'bonus' THEN sal.Amount ELSE NULL END) as Bonus,
                eml.Email
            FROM vlada_test.test_employees_dds as emp
            INNER JOIN vlada_test.test_salary_dds as sal
                 ON emp.ID = sal.ID
            INNER JOIN vlada_test.test_emails_dds as eml
                ON emp.ID = Emails.Empl_ID
            WHERE sal.dt >= '2020-01-01' AND sal.dt <= '2020-12-31'
            GROUP BY emp.ID, FIO, eml.Email
            """
        data = sqlio.read_sql_query(query, conn)
        conn.commit()
        print('SQL_TO_DATAFRAME DONE')
        print(data)
        return data

def load_final_report(table):
    data = get_data()
    with get_connect('pactg_db') as conn:
        engine = conn.get_sqlalchemy_engine()
        with engine.begin() as conn:
            # delete old data
            engine.execute(f'truncate table {table};')

            print(f'START_LOADING {table}')
            data.to_sql(table, engine, schema='vlada_test',
                                if_exists='append', index=False)
            
            print(f"LOADING SUCCESS {table}")



