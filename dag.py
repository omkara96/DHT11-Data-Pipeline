from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import oracledb

def execute_oracle_query():
    # Get the Oracle connection from Airflow
    oracle_conn = BaseHook.get_connection("OMKARADB01")

    # Construct the Oracle connection string
    wallet_path = oracle_conn.extra_dejson.get("wallet")
    service_name = oracle_conn.extra_dejson.get("service_name")
    wltpwd = oracle_conn.extra_dejson.get("wltpwd")
    cdir = oracle_conn.extra_dejson.get("cdir")
    dsn = oracle_conn.extra_dejson.get("dsn")
    print(f"wallet_path: {wallet_path}")
    print(f"service_name: {service_name}")
    print(f"wltpwd: {wltpwd}")
    print(f"cdir: {cdir}")
    print(f"DSN: {dsn}")	
    print(f"Connection: {oracle_conn.login}")
    print(f"Password: {oracle_conn.password}")
    print(f"DSN: {dsn}")
    
    with oracledb.connect(user=oracle_conn.login, password=oracle_conn.password, dsn=dsn, config_dir=cdir, wallet_location=wallet_path, wallet_password=wltpwd) as connection:
        with connection.cursor() as cursor:
            sql = """select * from esp_schema.esp_users """
            for r in cursor.execute(sql):
                print(r)

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('oracle_query_dag', default_args=default_args, schedule_interval=None, start_date=datetime(2022, 1, 1), catchup=False) as dag:
    execute_query_task = PythonOperator(
        task_id='execute_query_task',
        python_callable=execute_oracle_query,
    )
