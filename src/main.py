import os
import random
import json
import logging
from fastapi import FastAPI
from datetime import datetime
import mariadb
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import matplotlib.pyplot as plt

def get_mariadb_connection():
    try:
        connection = mariadb.connect(
            host=os.environ['MARIADB_HOST'],
            user=os.environ['MARIADB_USER'],
            password=os.environ['MARIADB_PASSWORD'],
            database=os.environ['MARIADB_DATABASE'],
        )
    except mariadb.Error as e:
        print(f"Error: {e}")
        return False

    return connection

def get_influxdb_client():
    try:
        client = InfluxDBClient(
            url='http://influxdb:8086',
            token=os.environ['INFLUXDB_TOKEN'],
            org=os.environ['INFLUXDB_ORG'],
            username=os.environ['INFLUXDB_USER'],
            password=os.environ['INFLUXDB_PASSWORD'],
            ssl=True,
            verify_ssl=True,
        )
    except Exception as e:
        print(f"Error: {e}")
        return False

    return client

def get_random_timestamp():
    start_timestamp = datetime(2024, 10, 1).timestamp()
    end_timestamp = datetime(2024, 10, 18).timestamp()
    random_timestamp = random.uniform(start_timestamp, end_timestamp)

    return datetime.fromtimestamp(random_timestamp)

def insert_event_with_random_timestamp_mariadb(timestamp):
    conn = get_mariadb_connection()
    if not conn:
        return False

    cursor = conn.cursor()
    cursor.execute('INSERT INTO dummy (timestamp) VALUES (%s)', (timestamp,))
    conn.commit()
    cursor.close()
    conn.close()

    return True
    

def insert_events_mariadb(events_data):
    """
    Inserts multiple events into the MariaDB database.

    Args:
        events_data (list): A list of dictionaries, each containing the following keys:
            - timestamp (datetime): The event timestamp.
            - message (str): A message describing the event (up to 255 characters).
            - severity_ID (int): The severity level ID.
            - event_type_ID (int): The event type ID.
            - source_ID (int): The source ID.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    # Database connection parameters
    db_config = {
        'host': os.environ.get('MARIADB_HOST', 'localhost'),
        'port': int(os.environ.get('MARIADB_PORT', 3306)),
        'user': os.environ.get('MARIADB_USER', 'root'),
        'password': os.environ.get('MARIADB_PASSWORD', ''),
        'database': os.environ.get('MARIADB_DATABASE', 'test_db')
    }

    try:
        # Establish the connection
        conn = mariadb.connect(**db_config)
        cursor = conn.cursor()

        # Validate message lengths
        for event_data in events_data:
            if len(event_data['message']) > 255:
                raise ValueError("Message length exceeds 255 characters.")

        # Insert query
        insert_query = """
        INSERT INTO Event (timestamp, message, severity_ID, event_type_ID, source_ID)
        VALUES (%s, %s, %s, %s, %s)
        """

        # Prepare data for insertion
        values = [
            (
                event_data['timestamp'],
                event_data['message'],
                event_data['severity_ID'],
                event_data['event_type_ID'],
                event_data['source_ID']
            )
            for event_data in events_data
        ]

        # Execute the query
        cursor.executemany(insert_query, values)

        # Commit the transaction
        conn.commit()
        # Close the connection
        cursor.close()
        conn.close()

        return True
    except (mariadb.Error, ValueError) as e:
        print(f"Error inserting events into MariaDB: {e}")
        return False






def insert_event_with_random_timestamp_influxdb(timestamp):
    client = get_influxdb_client()
    if not client:
        print("Failed to get InfluxDB client")
        return False

    try:
        #start_timestamp = datetime.now()
        write_api = client.write_api(write_options=SYNCHRONOUS)

        point = Point('dummy_measurement') \
            .time(timestamp) \
            .tag('source', 'fastapi_app') \
            .field('value', 1)

        write_api.write(
            bucket=os.environ['INFLUXDB_BUCKET'],
            org=os.environ['INFLUXDB_ORG'],
            record=point
        )
        #end_timestamp = datetime.now()

        print(f"Successfully wrote point to InfluxDB: {point.to_line_protocol()}")
        client.close()
        return True
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        print(f"Bucket: {os.environ['INFLUXDB_BUCKET']}")
        print(f"Org: {os.environ['INFLUXDB_ORG']}")
        print(f"Timestamp: {timestamp}")
        return False
        
def import_data_from_file(file_path):
    try:
        with open (file_path, 'r', encoding='utf-8-sig') as file:
            data = json.load(file)
            if isinstance(data, list):
                logging.info("Successfully extracted JSON array from file.")
                return data
            else:
                logging.error("Error: Expected a JSON array but found something else.")
    except FileNotFoundError:
        logging.error(f"Error: File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error: Failed to decode JSON: {e}")
        return []       

data = import_data_from_file("./data_1000.json")
for record in data:
    print(f"Record: {record}")

logging.basicConfig(level=logging.INFO)

app = FastAPI()

#@app.on_event("startup")
#def startup_event():
#    data = import_data_from_file("./data_1000.json")
#    for record in data:
#       result = insert_event_mariadb(record)
#       if (not result):
#           logging.warning("Error saving data in MariaDb")

# Update the FastAPI endpoint
@app.get('/')
def read_root():
    timestamp = get_random_timestamp()
    result_mariadb = insert_event_with_random_timestamp_mariadb(timestamp)
    if not result_mariadb:
        return {'message': 'Something went wrong with MariaDB!'}

    result_influxdb = insert_event_with_random_timestamp_influxdb(timestamp)
    if not result_influxdb:
        return {'message': 'Something went wrong with InfluxDB!'}

    return {'message': 'All good!'}
    
@app.get("/init")
def init_db():
    data = import_data_from_file("./data_100000.json")
    if (data == []):
        return {'message': 'JSON decoding error!'}
    time_durations = []
    for span in [1, 10, 100, 500, 1000, 2000, 5000]:
        temp_data = data[:span]
        timestamp_start = datetime.now()
        result = insert_events_mariadb(temp_data)
        if (not result):
            return {'message': 'Error saving data in MariaDb!'}
        timestamp_end = datetime.now()
        time_durations.append(timestamp_end - timestamp_start)

    time_durations_str = ', '.join(str(duration) for duration in time_durations)
    return {'message': f"All good! Time durations for each span: {time_durations_str}"}