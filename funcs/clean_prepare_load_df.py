from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extras import execute_values
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd
from airflow import DAG, XComArg

from funcs.csv_to_db_func import get_connect


def get_data(table):
    with get_connect('pactg_db') as conn:
        print('START_EXTRACT')
        query = 'SELECT * FROM {}'.format(table)
        data = sqlio.read_sql_query(query, conn)
        conn.commit()
        print('SQL_TO_DATAFRAME DONE')
        print(data)
        return data
        

def drop_duplicates(table, primary_key, ti = None):
    data = get_data(table)
    ti.xcom_push(key=table, value=len(data))
    # data.drop_duplicates(subset=['transaction_id'])
    data_without_duplicates = data.drop_duplicates(subset=primary_key)
    print(data_without_duplicates)
    print(len(data_without_duplicates))
    return data_without_duplicates

def transfer(table, primary_key, target_table):
    data = drop_duplicates(table, primary_key)

    with get_connect('pactg_db') as conn:
        engine = conn.get_sqlalchemy_engine()
        with engine.begin() as conn:
            # delete old data
            engine.execute(f'truncate table {target_table};')

            print(f'START_LOADING {target_table}')
            data.to_sql(target_table, engine, schema='vlada_test',
                                if_exists='append', index=False)
            
            print(f"LOADING SUCCESS {target_table}")


def check_table(table_dds, ti = None):
    bash_pushed_via_return_value = ti.xcom_pull(key="Hello_from_xcom", task_ids="bash_push")
    print(f'Print Xcom from bash_push {bash_pushed_via_return_value}')
    transfer_data_pushed_xcom = ti.xcom_pull(key=table_dds, task_ids="transfer_data")
    print(f'Print Xcom from transfer_data. Len datafrme = {transfer_data_pushed_xcom}') 
    if transfer_data_pushed_xcom > 0:
        print('Ckeck_success')
    else:
        print('Error load empty table')    
        raise ValueError

        


    


