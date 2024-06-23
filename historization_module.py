from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from custom_hooks.custom_oracle_hook import CustomOracleHook
from airflow.models.param import Param
from airflow.models import Variable  # Import Variable from airflow.models
from custom_hooks.custom_oracle_hook import CustomOracleHook
import sys
import os

# Add the directory of the current script to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Now you can import params without relative import issues
from HistProcessor import DeltaDetectionProcessor

input = {
    'dag_id': "Historization-Module",
    'email_notification': ['omkar.varma@merck.com', 'jp_hhie_dia_job@merck.com'],
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': None,
    'retry_count': 1,
    'sleep_time': 60
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

hook = CustomOracleHook(custom_conn_id="OMKARADB01")

def push_params(**kwargs):
    params = kwargs['dag_run'].conf
    print("Generating Global Parameters.....")
    #print(f"params ==> {params}:")
    print(f"Source Schmea                   => {params.get("source_schema")}:")
    print(f"Target Schmea                   => {params.get("target_schema")}:")
    print(f"Source Table                    => {params.get("source_table")}:")
    print(f"Target Table                    => {params.get("target_table")}:")
    print(f"Natural Keys                    => {params.get("natural_keys")}:")
    print(f"Source/Device_ID                => {params.get("device_id")}:")
    print(f"Columns to Exclude From Load    => {params.get("cols_to_exclude_from_load")}:")
    print(f"Columns to Exclude From Delta   => {params.get("columns_to_exclude_from_delta")}:")
    print(f"Load Key                        => {params.get("load_key")}:")

def process_delta_detection(**kwargs):
    print("Delta Detection process is starting......")
    params = kwargs['dag_run'].conf
    source_schema=params.get("source_schema")
    target_schema=params.get("target_schema")
    source_table=params.get("source_table")
    target_table=params.get("target_table")
    natural_keys=params.get("natural_keys")
    device_id=params.get("device_id")
    cols_to_exclude_from_load=params.get("cols_to_exclude_from_load")
    columns_to_exclude_from_delta=params.get("columns_to_exclude_from_delta")
    load_key=params.get("load_key")



with DAG(
    'Historization-Module',
    default_args=default_args,
    schedule_interval=input['schedule_interval'],
    start_date=input['start_date'],
    catchup=False,
    params={
        "source_schema": Param(description="Enter Intermediate Layer Schema",title="Source Schema "),
        "target_schema": Param(description="Enter Target Layer Schema",title="Target Schema "),
        "source_table": Param(description="Enter Target Table ",title="Source Table "),
        "target_table": Param(description="Enter Target Table ",title="Target Table "),
        "natural_keys": Param(type="array", description="Enter Natural Keys for SCD2 Load (Enter comma separated values)",title="Natural Keys (PK)"),
        "device_id": Param(description="Enter Source System or Device ID",title="Source/Device ID "),
        "cols_to_exclude_from_load": Param(type="array", description="Enter Columns to Exlude load from Target table load (Enter comma separated values)",title="Columns to Exlcude from TGT Table"),
        "columns_to_exclude_from_delta": Param(type="array", description="Enter Columns to Exclude from Delta Detection (Enter comma separated values)",title="Columns to Exclude from Delta Detection"),
        "load_key": Param(title="Load Key", description="Load Key to Process Historization")

    },

        # '''
        #     'source_schema': "ESP_SCHEMA", 
        #     'target_schema': "ESP_SCHEMA", 
        #     'source_table' : "DHT11_DATA_INT", 
        #     'target_table' : "HIST_DHT11_DATA", 
        #     'natural_keys' : "['DEVICEID', 'TIMESTAMP']", 
        #     'device_id'    : "DEV01OMKARVARMA",
        #     'cols_to_exclude_from_load'    : "['LOAD_KEY']",
        #     'columns_to_exclude_from_delta': "['TIMEZONE']",
        #     'load_key': '0'
        
) as dag:
    generate_parameters = PythonOperator(
        task_id='GET_GENERATE_GLOBAL_VARIABLES',
        python_callable=push_params,
        provide_context=True,
        op_kwargs={
            'source_schema': '{{ dag_run.conf.source_schema }}', 
            'target_schema': '{{ dag_run.conf.target_schema }}', 
            'source_table': '{{ dag_run.conf.source_table }}', 
            'target_table': '{{ dag_run.conf.target_table }}', 
            'natural_keys': '{{ dag_run.conf.natural_keys }}', 
            'device_id': '{{ dag_run.conf.device_id }}',
            'cols_to_exclude_from_load': '{{ dag_run.conf.cols_to_exclude_from_load }}',
            'columns_to_exclude_from_delta': '{{ dag_run.conf.columns_to_exclude_from_delta }}',
            'load_key': '{{ dag_run.conf.load_key }}'},
            dag=dag,
    )
    start_delta_detection = PythonOperator(
        task_id='PROCESS_DELTA_DETECTION',
        python_callable=process_delta_detection,
        provide_context=True,
        dag=dag,
    )

    generate_parameters >> start_delta_detection
