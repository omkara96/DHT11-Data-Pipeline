import dbconnect  # Assuming this module provides the database connection
import datetime

class InterfaceMetadata:

    def __init__(self, interface_name, interface_cd, load_status=None, load_start_dt_tm=None, load_complete_dt_tm=None, load_key=None):
        self.interface_name = interface_name
        self.interface_cd = interface_cd
        self.load_status = load_status
        self.load_start_dt_tm = load_start_dt_tm
        self.load_complete_dt_tm = load_complete_dt_tm
        self.load_key = load_key

    def _check_interface_existence(self):
        sql = f"""
            SELECT load_key, load_status
            FROM ESP_SCHEMA.data_control_table dct
            INNER JOIN esp_schema.interface_config ic 
            ON dct.INTERFACE_CD = ic.INTERFACE_CD 
            AND dct.INTERFACE_NAME = ic.INTERFACE_NAME
            WHERE dct.load_key IN (
                SELECT MAX(load_key)
                FROM ESP_SCHEMA.data_control_table
                WHERE INTERFACE_CD = '{self.interface_cd}'
            )
            AND dct.INTERFACE_CD = '{self.interface_cd}'
            
        """
        
        print("Executing SQL:", sql)

        try:
            cursor = dbconnect.get_cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            print("Query Result:", result)
            return result
        except Exception as e:
            print("Error executing query:", str(e))
            return None


    def _get_prev_run_dt_tm(self):
        sql = f"""
            SELECT load_key, load_status, LOAD_START_DT_TM
            FROM ESP_SCHEMA.data_control_table dct
            WHERE dct.INTERFACE_CD = '{self.interface_cd}'
            AND dct.load_key IN (
                SELECT MAX(load_key)
                FROM ESP_SCHEMA.data_control_table 
                WHERE INTERFACE_CD = '{self.interface_cd}' 
            )
        """
        
        print("Executing SQL:", sql)

        try:
            cursor = dbconnect.get_cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
            print("Query Result:", result)
            return result
        except Exception as e:
            print("Error executing query:", str(e))
            return None


    def add_current_run_entry(self, load_key, load_status):
        self.load_start_dt_tm = datetime.datetime.now()
        self.load_status = load_status
        self.load_key = load_key

        sql = f"""
            INSERT INTO ESP_SCHEMA.data_control_table (
                INTERFACE_NAME, INTERFACE_CD, LOAD_STATUS, LOAD_START_DT_TM, LOAD_KEY
            ) VALUES (
                :1, :2, :3, :4, :5
            )
        """

        print("Executing SQL:", sql)
        cursor = dbconnect.get_cursor()
        connection = cursor.connection 

        try:
            
            
            cursor.execute(sql, (
                self.interface_name, 
                self.interface_cd, 
                self.load_status, 
                self.load_start_dt_tm,
                self.load_key
            ))
            connection.commit()
            print("Current run entry added successfully.")
        except Exception as e:
            print("Error inserting current run entry:", str(e))
            if connection:
                connection.rollback()

    def update_current_run_entry(self, load_key, load_status=None, load_complete_dt_tm=None):
        # Set the load_status in the object
        self.load_status = load_status

        # Prepare the SQL statement dynamically based on whether load_complete_dt_tm is provided
        sql = f"""
            UPDATE ESP_SCHEMA.data_control_table
            SET LOAD_STATUS = '{load_status}', LOAD_COMPLETE_DT_TM= {load_complete_dt_tm} WHERE INTERFACE_CD = '{self.interface_cd}' and load_key = '{load_key}'
        """
        print("Executing SQL:", sql)
        cursor = dbconnect.get_cursor()
        connection = cursor.connection 

        try:
            cursor.execute(sql)
            connection.commit()
            print("Current run entry updated successfully.")
        except Exception as e:
            print("Error updating current run entry:", str(e))
            exit(1)
        