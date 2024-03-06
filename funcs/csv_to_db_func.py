
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas.io.sql as sqlio
from sqlalchemy import create_engine
import pandas as pd


def get_connect(conn_id):
    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        print("CONNECTION SUCCESS")
        return conn
    except Exception as error:
        raise AirflowException("ERROR: Connect error: {}".format(error))


def csv_to_db(table, csv):
    conn = PostgresHook(postgres_conn_id='pactg_db')
    engine = conn.get_sqlalchemy_engine()
    print("CONNECTION SUCCESS")
    engine = conn.get_sqlalchemy_engine()
    with engine.begin() as eng:
        df = pd.read_csv(csv)
        print("READ CSV SUCCESS")
        df.to_sql(table, eng, schema='vlada_test',
                  if_exists='append', index=False)
        print("DF TO SQL SUCCESS")
    conn.close()
