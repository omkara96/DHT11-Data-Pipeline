from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from custom_hooks.custom_oracle_hook import CustomOracleHook
#from custom_operators.custom_trigger_dag_run_operator import CustomTriggerDagRunOperator
from airflow.models import Variable  # Import Variable from airflow.models
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import db
import sys
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import os

# Add the directory of the current script to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Now you can import params without relative import issues
import params

firebase_creds = Variable.get("FirebaseCreds")

# Convert the Firebase credentials to dictionary
firebase_creds_dict = eval(firebase_creds, {
    'databaseURL': Variable.get("FirebaseDatabaseURL")
    })

# Initialize Firebase Admin SDK
firebase_cred = credentials.Certificate(firebase_creds_dict)
firebase_admin.initialize_app(firebase_cred, {
    'databaseURL': Variable.get("FirebaseDatabaseURL")
    })

input = {
    'dag_id': "DHT11_DATA_2_ORACLE_E2E",
    'email_notification': ['omkar.varma@merck.com', 'jp_hhie_dia_job@merck.com'],
    'start_date': datetime(2022, 1, 1),
    'schedule_interval': "30 09 * * *",
    'retry_count': 1,
    'sleep_time': 60
}
hook = CustomOracleHook(custom_conn_id="OMKARADB01")
G_C_LOAD_KEY = None
def push_params(**kwargs):
    print("Generating Global Parameters.....")
    print(Variable.get("FirebaseCreds"))
    print(Variable.get("FirebaseDatabaseURL"))
    for pkey in params.config:
        print(f"key: {pkey} => Value: {params.config.get(pkey)}")
        kwargs['ti'].xcom_push(key=pkey, value=params.config.get(pkey))

def get_conf(context, dag_run_obj):
	dag_run_obj.payload = replace_placeholders_json(context, context['params'])
	dag_run_id = str(dag_run_obj.run_id)
	dag_id = str(dag_run_obj.dag_id)
	print("DAG run object payload (input parameters):")
	print(dag_run_obj.payload)
	print("DAG run object run_id: " + dag_run_id)
	print("DAG run object dag_id: " + dag_id)
	dag_run_id_key = 'dag_run_id_' + str(context['ti'].task_id)
	dag_id_key = 'dag_id_' + str(context['ti'].task_id)
	context['ti'].xcom_push(key=dag_run_id_key, value=dag_run_id)
	context['ti'].xcom_push(key=dag_id_key, value=dag_id)
	return dag_run_obj

def get_status_of_load(load_key, interface_cd):
    print(f"Getting Status of Interface: {interface_cd}. Load Key is : {load_key}")
    sql = f"""
            SELECT load_status
            FROM ESP_SCHEMA.data_control_table dct
            WHERE dct.INTERFACE_CD = '{interface_cd}'
            AND dct.load_key = '{load_key}
        """
    try:
            print("Executing SQL:", sql)
            cursor = hook.get_cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            print("Query Result:", result)
            if result:
                status = result[0]
                status = status[0]
                return status
            else :
                 print(f"DB Error Unable to fetch records, Cursor Result {result}")

    except Exception as e:
            print("Error executing query:", str(e))
            exit(1)

def update_status_of_load(load_key, interface_cd, status):
    print("Updating Control Table.....")
    print(f"Getting Status of Interface: {interface_cd}. Load Key is : {load_key}")
    sql = f"""
            UPDATE 
            ESP_SCHEMA.data_control_table SET load_status='{status}'
            WHERE INTERFACE_CD = '{interface_cd}'
            AND load_key = '{load_key}'
        """
    try:
            print("Executing SQL:", sql)
            cursor = hook.get_cursor()
            connection = cursor.connection
            cursor.execute(sql)
            connection.commit()
            print(f"Status Updated Successfully....")
            
    except Exception as e:
            print("Error executing query:", str(e))
            exit(1)

# Define a Python function to determine the branching logic
def check_data_list(**kwargs):
    data_list = kwargs['ti'].xcom_pull(task_ids='GET_FIREBASE_DELTA_DHT_DATA', key='DHT_DATA')
    if not data_list:
        return "skip_trigger_dag_task"
    else:
        return "DHT_DATA_FIREBASE_TO_DB_LANDING_LAYER"



def __print_variables_and_gen_laod_key(**kwargs):
     print("===================== PRINT LOAD VARIABLES =====================")
     interface_cd = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Interface_cd')
     PREV_LOAD_KEY = kwargs['ti'].xcom_pull(task_ids='GET_ETL_PREV_RUN_DATE_TIME', key='PREV_LOAD_KEY')
     ETL_PREV_RUN_DTTM = kwargs['ti'].xcom_pull(task_ids='GET_ETL_PREV_RUN_DATE_TIME', key='ETL_PREV_RUN_DTTM')
     Interface_NM = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Interface_NM')
     DEVID = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Device_Id')
     C_LOAD_KEY = int(PREV_LOAD_KEY) + 1
     print(f"Interface Name                      :{Interface_NM}")
     print(f"Interface Code                      :{interface_cd}")
     print(f"ETL Previous Load Key               :{PREV_LOAD_KEY}")
     print(f"ETL Previous Run DateTime           :{ETL_PREV_RUN_DTTM}")
     print(f"Device ID / Source System           :{DEVID}")
     print(f"Current Run Load Key                :{C_LOAD_KEY}")
     print("================================================================")
     kwargs['ti'].xcom_push(key="C_LOAD_KEY", value=C_LOAD_KEY)
     
     sql = f"""
            INSERT INTO ESP_SCHEMA.data_control_table (
                INTERFACE_NAME, INTERFACE_CD, LOAD_STATUS, LOAD_START_DT_TM, LOAD_KEY
            ) VALUES (
                '{Interface_NM}', '{interface_cd}', 'APP SPECIFIC LOADING', SYSDATE, '{C_LOAD_KEY}'
            )
        """

     print("Executing SQL:", sql)
     cursor = hook.get_cursor()
     connection = cursor.connection 

     try:
            cursor.execute(sql)
            connection.commit()
            print("Current run entry added successfully.")
     except Exception as e:
            print("Error inserting current run entry:", str(e))
            if connection:
                connection.rollback()
     pass
     

def get_etl_previous_run_date(**kwargs):
    print("Getting ETL Previous Run Date ................................")
    interface_cd = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Interface_cd')
    
    sql = f"""
            SELECT load_key, load_status, LOAD_START_DT_TM
            FROM ESP_SCHEMA.data_control_table dct
            WHERE dct.INTERFACE_CD = '{interface_cd}'
            AND dct.load_key IN (
                SELECT MAX(load_key)
                FROM ESP_SCHEMA.data_control_table 
                WHERE INTERFACE_CD = '{interface_cd}' 
            )
        """
    print("Executing SQL:", sql)
    try:
            cursor = hook.get_cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            print("Query Result:", result)
            if result:
                prev_run = result[0]
                print("Getting Previous Load Key and Previous Run DateTime")
                print(f"Load Key: {prev_run[0]}")
                print(f"Load Status: {prev_run[1]}")
                print(f"Load Prev Run Date Time: {prev_run[2]}")
                if prev_run[1] == "Success" :
                    kwargs['ti'].xcom_push(key="PREV_LOAD_KEY", value=prev_run[0])
                    kwargs['ti'].xcom_push(key="ETL_PREV_RUN_DTTM", value=prev_run[2])
                    print(f"ETL Previos Run Date: {prev_run[2]}")
                    print(f"ETL Previos Load Key: {prev_run[0]}")
                    pass
                else:
                    print(f"Previous Load was not completed successfully. Please check the previous load.\nPrevious Load Status: {prev_run[1]}")
                    exit(1)
            else:
                print("No previous run data found. Please add initial entries in control tables...")
                exit(1)
    except Exception as e:
            print("Error executing query:", str(e))
            exit(1)

def _check_interface_existence(**kwargs):
    print("Checking for Interface Existance in Control Tables..............................")
    ti = kwargs['ti']
    interface_cd = ti.xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Interface_cd')
    sql = f"""
        SELECT load_key, load_status
        FROM ESP_SCHEMA.data_control_table dct
        INNER JOIN ESP_SCHEMA.interface_config ic 
        ON dct.INTERFACE_CD = ic.INTERFACE_CD 
        AND dct.INTERFACE_NAME = ic.INTERFACE_NAME
        WHERE dct.load_key IN (
            SELECT MAX(load_key)
            FROM ESP_SCHEMA.data_control_table
            WHERE INTERFACE_CD = '{interface_cd}'
        )
        AND dct.INTERFACE_CD = '{interface_cd}'
    """
    print("Executing SQL:", sql)

    
    try:
        cursor = hook.get_cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        print("Interface Exists Last Load Key and Load Status:", result)
        if not result:
            raise ValueError("No entry found in config tables for the interface given. Please recheck parameters or create starting entries.")
        else:
            print("Entries found in control table.")
    except Exception as e:
        raise RuntimeError(f"Error executing query: {str(e)}")
    
def truncate_data_landing_table(**kwargs):
    device_id = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Device_Id')
    try:
        cursor = hook.get_cursor()
        if cursor is None:
            print("Failed to get database cursor.")
            exit(1)
        
        connection = cursor.connection  # Get the connection from the cursor
        print(f"Deleting Staging data for Device ID : {device_id}")
        cursor.execute(f"""
                DELETE FROM ESP_SCHEMA.DHT11_DATA WHERE DEVICEID = '{device_id}'
            """)

        connection.commit()
        pass

    except Exception as e:
        print("Error executing query:", str(e))
        exit(1)

def get_data_since_timestamp(**kwargs):
    print("Initiating Firebase Logging in.....")

    # Initialize Firebase Admin SDK
    if not firebase_admin._apps:
        # Fetch Firebase credentials from Airflow Variables
        firebase_creds = Variable.get("FirebaseCreds")
        firebase_creds_dict = eval(firebase_creds)
        firebase_cred = credentials.Certificate(firebase_creds_dict)
        firebase_admin.initialize_app(firebase_cred, {
    'databaseURL': Variable.get("FirebaseDatabaseURL")
    })

    print("Connecting to Firebase Database via API.............")
    Firebase_Data_Node = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Firebase_Data_Node')
    print(f"Firebase Data Node: {Firebase_Data_Node}")
    Firebase_Parent_Node = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Firebase_Parent_Node')
    print(f"Firebase Parent Node: {Firebase_Parent_Node}")
    device_id = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Device_Id')
    print(f"Firebase Device ID : {device_id}")
    ETL_PREV_RUN_DTTM = kwargs['ti'].xcom_pull(task_ids='GET_ETL_PREV_RUN_DATE_TIME', key='ETL_PREV_RUN_DTTM')
    threshold_timestamp = str(ETL_PREV_RUN_DTTM)
    print(f"Fetching DHT Data from Firebase >= {threshold_timestamp}")
    
    # Get a reference to the Firebase database node
    ref = db.reference(Firebase_Parent_Node).child(device_id).child(Firebase_Data_Node)
    all_data = ref.get()
    
    if not all_data:
        print("No data found in the database.")
        return []

    threshold_dt = datetime.strptime(threshold_timestamp, '%Y-%m-%d %H:%M:%S')
    filtered_data = []
    print("Preparing Data List......")
    for date, times in all_data.items():
        for time, data in times.items():
            data_timestamp = datetime.strptime(data.get('Timestamp', '1970-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S')
            if data_timestamp >= threshold_dt:
                filtered_data.append(data)

    kwargs['ti'].xcom_push(key="DHT_DATA", value=filtered_data)




def insert_data_to_oracle(**kwargs):
    interface_cd = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Interface_cd')
    C_LOAD_KEY = kwargs['ti'].xcom_pull(task_ids='PRINT_VARIABLES_AND_GENERATE_NEW_LOAD_KEY', key='C_LOAD_KEY')
    data_list = kwargs['ti'].xcom_pull(task_ids='GET_FIREBASE_DELTA_DHT_DATA', key='DHT_DATA')
    device_id = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Device_Id')
    if data_list:
        try:
            print("Data Insertion from Firebase Database to Oracle Database...........")
            cursor = hook.get_cursor()
            if cursor is None:
                print("Failed to get database cursor.")
                exit(1)
            #data_list, device_id = get_data_since_timestamp()
            connection = cursor.connection  # Get the connection from the cursor
            print(f"Total number of rows to insert: {len(data_list)}")

            for i, data in enumerate(data_list, start=1):
                cursor.execute("""
                    INSERT INTO ESP_SCHEMA.DHT11_DATA (TimeZone, Humidity, Temperature, Timestamp, DEVICEID)
                    VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD HH24:MI:SS'), :5)
                """, (
                    data.get('TimeZone', 'N/A'),
                    data.get('Humidity', 'N/A'),
                    data.get('Temperature', 'N/A'),
                    data.get('Timestamp', 'N/A'),
                    device_id
                ))

                #print(f"Inserted row {i}/{len(data_list)}")
            print(f"{len(data_list)} Inserted Succssfully....")
            connection.commit()
            update_status_of_load(C_LOAD_KEY, interface_cd, "APP SPECIFIC LOADING CMPLT")
            pass

        except Exception as e:
            update_status_of_load(C_LOAD_KEY, interface_cd, "APP SPECIFIC LOADING FAILED")
            print("Error executing query:", str(e))
            exit(1)
    else:
         print("Error in Data Retrival from XCOM or Last task....\nAborting....")
         exit(1)

def get_column_names_from_table(schema_name, Table_Name):
     print(f"\n\nGetting Column Names for Table: {schema_name}.{Table_Name}")
     cursor = hook.get_cursor()
     if cursor is None:
        print("Failed to get database cursor.")
        exit(1)
            
     connection = cursor.connection  # Get the connection from the cursor
     sql = f"""
        SELECT COLUMN_NAME
        FROM all_tab_cols
        WHERE table_name = '{Table_Name}' 
        AND OWNER = '{schema_name}'  and COLUMN_NAME NOT LIKE 'SYS_%'

         """
     print(f"Executng SQL {sql}")
     cursor.execute(sql)
     res_src_col = cursor.fetchall()
     src_table_col = sorted(column[0] for column in res_src_col)
     src_table_col_str = ", ".join(src_table_col)
     print("Table Column Names...\n")
     print(src_table_col_str)
     return src_table_col_str

def update_and_finish(**kwargs):
        C_LOAD_KEY = kwargs['ti'].xcom_pull(task_ids='PRINT_VARIABLES_AND_GENERATE_NEW_LOAD_KEY', key='C_LOAD_KEY')
        device_id = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Device_Id')
        interface_cd = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='interface_cd')
        landing_table = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='landing_table')
        landing_schema = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='landing_schema')
        intermediate_schema = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='intermediate_schema')
        intermediate_table = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='intermediate_table')
        interface_cd = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Interface_cd')
        print("Processing Intermediate Table load from Staging/Landing...................")
        print(f"Load Key             :{C_LOAD_KEY}")
        print(f"DEV ID               :{device_id}")
        print(f"LND TBL              :{landing_table}")
        print(f"LND Schema           :{landing_schema}")
        print(f"INT Schema           :{intermediate_schema}")
        print(f"INT Table            :{intermediate_table}")
        print(f"Interface Code       :{interface_cd}")
        cursor = hook.get_cursor()
        connection = cursor.connection
        update_status_of_load(C_LOAD_KEY, interface_cd, "Success")
        cursor.execute(f""" UPDATE ESP_SCHEMA.HIST_LOAD_CONTROL  SET
                        
                        status ='processed',
                        end_date= SYSDATE 
    where load_key= '{C_LOAD_KEY}' and SUBJECT_AREA= '{device_id}' """)
        connection.commit()
        cursor.execute(f"""
            UPDATE 
            ESP_SCHEMA.data_control_table SET load_status='Success', LOAD_COMPLETE_DT_TM=SYSDATE
            WHERE INTERFACE_CD = '{interface_cd}'
            AND load_key = '{C_LOAD_KEY}'
        """)
        connection.commit()
     
def staging_to_int_load_process(**kwargs):
    try:
        C_LOAD_KEY = kwargs['ti'].xcom_pull(task_ids='PRINT_VARIABLES_AND_GENERATE_NEW_LOAD_KEY', key='C_LOAD_KEY')
        device_id = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='Device_Id')
        interface_cd = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='interface_cd')
        landing_table = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='landing_table')
        landing_schema = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='landing_schema')
        intermediate_schema = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='intermediate_schema')
        intermediate_table = kwargs['ti'].xcom_pull(task_ids='GET_GENERATE_GLOBAL_VARIABLES', key='intermediate_table')
        print("Processing Intermediate Table load from Staging/Landing...................")
        print(f"Load Key             :{C_LOAD_KEY}")
        print(f"DEV ID               :{device_id}")
        print(f"LND TBL              :{landing_table}")
        print(f"LND Schema           :{landing_schema}")
        print(f"INT Schema           :{intermediate_schema}")
        print(f"INT Table            :{intermediate_table}")
        landing_column_names = get_column_names_from_table(landing_schema, landing_table)
        G_C_LOAD_KEY = C_LOAD_KEY
        sql = f"""
                INSERT INTO {intermediate_schema}.{intermediate_table} ({landing_column_names}, load_key)
                SELECT {landing_column_names}, :1
                FROM {landing_schema}.{landing_table}
            """
        print(sql)

        cursor = hook.get_cursor()
        
        if cursor is None:
            print("Failed to get database cursor.")
            exit(1)
        connection = cursor.connection
        cursor.execute(sql,(C_LOAD_KEY,))

        print("Inserted data into Intermediate Table.....")

        cursor.execute(f""" insert into ESP_SCHEMA.HIST_LOAD_CONTROL (
                        load_key  ,
                        subject_area ,
                        status ,
                        start_date ,
                        end_date ,
                        inserted_datetime ) values ('{C_LOAD_KEY}', '{device_id}', 'unprocessed', SYSDATE, NULL, SYSDATE) """)
        
        print("Added Hist Load Control Entry......")
        update_status_of_load(C_LOAD_KEY, interface_cd, "INT LOAD COMPLT")
        connection.commit()
    except Exception as e:
            connection.rollback()
            update_status_of_load(C_LOAD_KEY, interface_cd, "INT LOAD FAILED")
            print("Error executing query:", str(e))
            connection.commit()
            exit(1)
    pass

def execute_oracle_query():
    hook = CustomOracleHook(custom_conn_id="OMKARADB01")
    cursor = hook.get_cursor()
    cursor.execute("SELECT * FROM ESP_SCHEMA.esp_users")
    results = cursor.fetchall()
    for row in results:
        print(row)
    cursor.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'DHT11_DATA_2_ORACLE_E2E',
    default_args=default_args,
    schedule_interval=input['schedule_interval'],
    start_date=input['start_date'],
    catchup=False
) as dag:
    generate_parameters = PythonOperator(
        task_id='GET_GENERATE_GLOBAL_VARIABLES',
        python_callable=push_params,
        provide_context=True,
    )
    execute_query_task = PythonOperator(
        task_id='execute_query_task',
        python_callable=execute_oracle_query,
        provide_context=True,
    )
    check_interface_task = PythonOperator(
        task_id='CHECK_FOR_INTERFACE_EXISANCE',
        python_callable=_check_interface_existence,
        provide_context=True,
    )
    get_etl_previous_run_date_time = PythonOperator(
        task_id='GET_ETL_PREV_RUN_DATE_TIME',
        python_callable=get_etl_previous_run_date,
        provide_context=True,
    )
    print_variables_and_gen_new_load_key = PythonOperator(
        task_id='PRINT_VARIABLES_AND_GENERATE_NEW_LOAD_KEY',
        python_callable=__print_variables_and_gen_laod_key,
        provide_context=True,
    )

    delete_existing_landing_data  = PythonOperator(
        task_id='TRUNCATE_EXISTING_LANDING_DATA',
        python_callable=truncate_data_landing_table,
        provide_context=True,
    )
    get_firebase_delta_data  = PythonOperator(
        task_id='GET_FIREBASE_DELTA_DHT_DATA',
        python_callable=get_data_since_timestamp,
        provide_context=True,
    )
    firebase_to_db_landing  = PythonOperator(
        task_id='DHT_DATA_FIREBASE_TO_DB_LANDING_LAYER',
        python_callable=insert_data_to_oracle,
        provide_context=True,
    )
    landing_to_intermediate_table  = PythonOperator(
        task_id='LANDING_TO_INTERMEDIATE_TABLE',
        python_callable=staging_to_int_load_process,
        provide_context=True,
    )
    t_trigger_dag_wf_ext_historization_flow =  TriggerDagRunOperator(
        task_id='trigger_DAG_WF_EXT_SCD2_HISTORIZATION_LOAD',
        trigger_dag_id='Historization-Module',  # Replace with the ID of the DAG to trigger
        conf={
    "cols_to_exclude_from_load": [
        'LOAD_KEY'
    ],
    "columns_to_exclude_from_delta": [
        'TIMEZONE'
    ],
    "device_id": "DEV01OMKARVARMA",
    "load_key": "{{ task_instance.xcom_pull(task_ids='PRINT_VARIABLES_AND_GENERATE_NEW_LOAD_KEY', key='C_LOAD_KEY') }}",
    "natural_keys": [
        'DEVICEID', 'TIMESTAMP'
    ],
    "source_schema": "ESP_SCHEMA",
    "source_table": "DHT11_DATA_INT",
    "target_schema": "ESP_SCHEMA",
    "target_table": "HIST_DHT11_DATA"
},  # Optional: pass configuration to the triggered DAG
       # execution_date='{{ ds }}',  # Optional: pass execution date context
        reset_dag_run=True,  # Optional: reset DAG run if it already exists
        wait_for_completion=True,  # Optional: wait for DAG run to complete
        poke_interval=60,  # Optional: interval to check the DAG run status
        allowed_states=['success'],  # Optional: states to consider as success
        failed_states=['failed'],  # Optional: states to consider as failure
    )
    update_and_finish = PythonOperator(
        task_id='UPDATE_STATUS_AND_FINISH_WORKFLOW',
        python_callable=update_and_finish,
        provide_context=True,
        trigger_rule='none_failed',
    )
    # Create a BranchPythonOperator to determine the path based on data_list
    check_data_list_task = BranchPythonOperator(
    task_id='check_data_list_task',
    python_callable=check_data_list,
    provide_context=True,

    )
    skip_trigger_dag_task = DummyOperator(task_id='skip_trigger_dag_task')


  

    get_firebase_delta_data >> check_data_list_task >> [skip_trigger_dag_task, firebase_to_db_landing]
    generate_parameters >> execute_query_task >> check_interface_task >> get_etl_previous_run_date_time >> print_variables_and_gen_new_load_key >> delete_existing_landing_data >> get_firebase_delta_data 
    skip_trigger_dag_task >>   update_and_finish 
    firebase_to_db_landing >> landing_to_intermediate_table >> t_trigger_dag_wf_ext_historization_flow >>  update_and_finish
