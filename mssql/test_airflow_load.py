"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'mrice',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

database_server = './sqlexpress'
database_name = 'database_name'

dag = DAG('CSVDataLoad', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='run_python_csv_load',
    bash_command=f"python load_csvs_mssql.py {DatabaseServer} {database_name}",dag=dag)

t2 = MsSqlOperator(
    task_id='execute_ref_sprocs',
    mssql_conn_id = 'airflow_mssql',
    sql = "EXEC {}.ref.ReferenceBuild".format(DatabaseServer),
    retries=1,
    dag=dag,
    autocommit=True)


t2.set_upstream(t1)
