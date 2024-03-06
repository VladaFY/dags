import datetime

from airflow import DAG, XComArg
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from DDS_dag.clean_prepare_load_df import transfer, check_table


with DAG(dag_id='source_to_dds',
         schedule_interval="0 2 * * *",
         start_date=datetime(2024, 3, 1),
         catchup=False) as dag:

    start_step = DummyOperator(task_id='start_step')

        
    check_table_task_employees = PythonOperator(
        task_id='check_table_task_employees', python_callable=check_table
        , op_kwargs={'table_dds': 'vlada_test.test_employees'})
    
    transfer_data_employees = PythonOperator(
        task_id='transfer_data_employees', python_callable=transfer
        , op_kwargs={'table': 'vlada_test.test_employees'
                     , 'target_table': 'vlada_test.test_employees_dds', 'primary_key': 'id'})
    
    check_table_task_salary = PythonOperator(
        task_id='check_table_task_salary', python_callable=transfer
        , op_kwargs={'table_dds': 'vlada_test.test_salary'})
    
    transfer_data_salary = PythonOperator(
        task_id='transfer_data', python_callable=transfer
        , op_kwargs={'table': 'vlada_test.test_salary'
                     , 'target_table': 'vlada_test.test_salary_dds'
                     , 'primary_key': ['id', 'dt', 'salary_Type']})
     
    check_table_task_emails = PythonOperator(
        task_id='check_table_task_emails', python_callable=check_table
        , op_kwargs={'table_dds': 'vlada_test.test_emails'})
    
    transfer_data_emails = PythonOperator(
        task_id='transfer_data_emails', python_callable=transfer
        , op_kwargs={'table': 'vlada_test.test_emails'
                     , 'target_table': 'vlada_test.test_emails_dds', 'primary_key': 'id'})
        
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command='echo "bash_push demo"  && '
        'echo "Manually set xcom value '
        '{{ ti.xcom_push(key="Hello_from_xcom", value="Hello_from_xcom") }}" && '
        'echo "value_by_return"',
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        bash_command='echo "bash pull demo" && '
        f'echo "The xcom pushed manually is {XComArg(bash_push, key="Hello_from_xcom")}" && '
        f'echo "The returned_value xcom is {XComArg(bash_push)}" && '
        'echo "finished"',
        do_xcom_push=False,
    )

start_step >> bash_push >> transfer_data_employees >> check_table_task_employees  >> bash_pull
start_step >> bash_push >> transfer_data_salary >> check_table_task_salary  >> bash_pull
start_step >> bash_push >> transfer_data_emails >> check_table_task_emails  >> bash_pull