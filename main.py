import params
from CheckInterface_Metadata import InterfaceMetadata
import STG_Firebase2Oracle 
import STG_to_INT
import datetime

def get_previous_run_details(metadata_obj):
    prev_run_result = metadata_obj._get_prev_run_dt_tm()
    if prev_run_result:
        prev_run = prev_run_result[0]
        print("Getting Previous Load Key and Previous Run DateTime")
        print(f"Load Key: {prev_run[0]}")
        print(f"Load Status: {prev_run[1]}")
        print(f"Load Prev Run Date Time: {prev_run[2]}")
        if prev_run[1] == "Success" :
            print("Starting Load Preparations....")
            return prev_run[2], prev_run[0]
        else:
            print("Previous Load was not completed successfully. Please check the previous load.")
            exit(1)
    else:
        print("No previous run data found. Please add initial entries in control tables...")
        exit(1)

def init():
    Device_ID = params.config.get("Device_Id")
    print(f"Starting Batch Data DHT 11 Data Sync for Device_ID: {Device_ID}")
    print("Initializing Metadata check, please wait...")

    interface_nm = params.config.get("Interface_NM")
    interface_cd = params.config.get("Interface_cd")
    metadata_obj = InterfaceMetadata(interface_nm, interface_cd)

    status = metadata_obj._check_interface_existence()
    if not status:
        print("No entry found in config tables for the interface given. Please recheck parameters or create starting entries.")
        exit(1)
    else:
        print("Entries found in control table.")
        print("Getting previous batch run details...")
        prev_run_details = get_previous_run_details(metadata_obj)
        if prev_run_details:
            prev_run_date_time, load_key = prev_run_details
            print(f"ETL Previous Run Date: {prev_run_date_time}")
            print(f"Load Key: {load_key}")
            # Adding current run entry
            metadata_obj.add_current_run_entry(load_key + 1, "APP SPECIFIC LOADING")
            # Assuming some ETL process here
            # After ETL process completes successfully, update the current run entry
            data_list = STG_Firebase2Oracle.get_data_since_timestamp(prev_run_date_time)
            if data_list:
                del_res=STG_Firebase2Oracle.delete_stg_data(Device_ID)
                if del_res ==0:
                    print("Staging Data delete succssfully..... \nproceeding for next load....")
                    result = STG_Firebase2Oracle.insert_data_to_oracle(data_list, Device_ID)
                    if result == 0:
                        print("Data inserted into Oracle database successfully.")
                        metadata_obj.update_current_run_entry(load_key + 1, "APP SPECIFIC LOADING COMPLETED", "NULL")
                        intLoadKey = load_key + 1
                        result = STG_to_INT.load_data_to_int_table(intLoadKey, Device_ID)
                        if result == 0:
                            print("Data loaded successfully.")
                        else:
                            print("Failed to load data")
                        metadata_obj.update_current_run_entry(load_key + 1, "INTEGRATION LOAD COMPLETED", "NULL")
                    else:
                        print("Failed to insert data into Oracle database.")
                        metadata_obj.update_current_run_entry(load_key + 1, "APP SPECIFIC LOADING FAILED", "NULL")
                else:
                    print("Error In deleting STG Data...")
                    metadata_obj.update_current_run_entry(load_key + 1, "APP SPECIFIC LOADING FAILED", "NULL")
                    exit(1)
            else:
                print("No data to insert.")
                metadata_obj.update_current_run_entry(load_key + 1, "APP SPECIFIC LOADING COMPLETED", "NULL")
            
        else:
            print("Error in getting previous run date time...")
            exit(1)

if __name__ == "__main__":
    init()
