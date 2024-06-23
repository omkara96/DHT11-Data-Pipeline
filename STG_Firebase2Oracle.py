import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin import db
from datetime import datetime
import dbconnect
# Initialize Firebase Admin SDK (replace 'path/to/your/serviceAccountKey.json' with your service account key path)
cred = credentials.Certificate('firebase_credentials.json')

firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://esp32-widgets-default-rtdb.asia-southeast1.firebasedatabase.app/'
}) 


def __display_since_delta_data(threshold_timestamp):
    ref = db.reference('MCU_Data').child('DEV01OMKARVARMA').child('HIST_DHT11_DATA')
    all_data = ref.get()

    if not all_data:
        print("No data found in the database.")
        return

    threshold_dt = datetime.strptime(threshold_timestamp, '%Y-%m-%d %H:%M:%S')
    filtered_data = []

    for date, times in all_data.items():
        for time, data in times.items():
            data_timestamp = datetime.strptime(data.get('Timestamp', '1970-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S')
            if data_timestamp >= threshold_dt:
                filtered_data.append(data)

    if not filtered_data:
        print(f"No data found with timestamp >= {threshold_timestamp}")
    else:
        for i, data in enumerate(filtered_data, start=1):
            print(f"Record {i}:")
            print(f"1. TimeZone: {data.get('TimeZone', 'N/A')}")
            print(f"2. Humidity: {data.get('Humidity', 'N/A')}")
            print(f"3. Temperature: {data.get('Temperature', 'N/A')}")
            print(f"4. Timestamp: {data.get('Timestamp', 'N/A')}")
            print()


def get_data_since_timestamp(threshold_timestamp):
    threshold_timestamp = str(threshold_timestamp)
    ref = db.reference('MCU_Data').child('DEV01OMKARVARMA').child('HIST_DHT11_DATA')
    all_data = ref.get()

    if not all_data:
        print("No data found in the database.")
        return []

    threshold_dt = datetime.strptime(threshold_timestamp, '%Y-%m-%d %H:%M:%S')
    filtered_data = []

    for date, times in all_data.items():
        for time, data in times.items():
            data_timestamp = datetime.strptime(data.get('Timestamp', '1970-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S')
            if data_timestamp >= threshold_dt:
                filtered_data.append(data)

    return filtered_data



def insert_data_to_oracle(data_list, device_id):
    try:
        cursor = dbconnect.get_cursor()
        if cursor is None:
            print("Failed to get database cursor.")
            return None
        
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

            print(f"Inserted row {i}/{len(data_list)}")

        connection.commit()
        cursor.close()
        connection.close()
        return 0

    except Exception as e:
        print("Error executing query:", str(e))
        return None


def delete_stg_data(device_id):
    try:
        cursor = dbconnect.get_cursor()
        if cursor is None:
            print("Failed to get database cursor.")
            return None
        
        connection = cursor.connection  # Get the connection from the cursor
        print(f"Deleting Staging data for Device ID : {device_id}")
        cursor.execute(f"""
                DELETE FROM ESP_SCHEMA.DHT11_DATA WHERE DEVICEID = '{device_id}'
            """)

        connection.commit()
        cursor.close()
        connection.close()
        return 0

    except Exception as e:
        print("Error executing query:", str(e))
        return None

    
# Example usage
# threshold_timestamp = '2024-05-05 11:30:35'
# data_list = get_data_since_timestamp(threshold_timestamp)

# if data_list:
#     del_res=delete_stg_data('DEV01OMKARVARMA')
#     if del_res ==0:
#         print("Staging Data delete succssfully..... \nproceeding for next load....")
#         result = insert_data_to_oracle(data_list, 'DEV01OMKARVARMA')
#         if result == 0:
#             print("Data inserted into Oracle database successfully.")
#         else:
#             print("Failed to insert data into Oracle database.")
#     else:
#         print("Error In deleting STG Data...")
#         exit(1)
# else:
#     print("No data to insert.")