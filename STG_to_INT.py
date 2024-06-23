import dbconnect

def load_data_to_int_table(load_key, device_id):
    try:
        cursor = dbconnect.get_cursor()
        if cursor is None:
            print("Failed to get database cursor.")
            return None
        
        connection = cursor.connection  # Get the connection from the cursor

        # Create a new load key for the current run
        print(f"Creating new load key: {load_key}")

        # Insert data into ESP_SCHEMA.DHT11_DATA_INT with the same load key for all rows
        cursor.execute("""
            INSERT INTO ESP_SCHEMA.DHT11_DATA_INT (TimeZone, Humidity, Temperature, Timestamp, DEVICEID, load_key)
            SELECT TimeZone, Humidity, Temperature, Timestamp, DEVICEID, :1
            FROM ESP_SCHEMA.DHT11_DATA
        """, (load_key,))


        cursor.execute(f""" insert into ESP_SCHEMA.HIST_LOAD_CONTROL (
                    load_key  ,
                    subject_area ,
                    status ,
                    start_date ,
                    end_date ,
                    inserted_datetime ) values ('{load_key}', '{device_id}', 'unprocessed', SYSDATE, NULL, SYSDATE) """)

        connection.commit()
        cursor.close()
        connection.close()
        return 0

    except Exception as e:
        print("Error executing query:", str(e))
        return None

# # Example usage
# load_key = '7'
# result = load_data_to_int_table(load_key, "DEV01OMKARVARMA")
# if result == 0:
#     print("Data loaded into ESP_SCHEMA.DHT11_DATA_INT successfully.")
# else:
#     print("Failed to load data into ESP_SCHEMA.DHT11_DATA_INT.")
