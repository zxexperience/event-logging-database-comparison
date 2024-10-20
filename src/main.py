import os
import random
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

def insert_event_with_random_timestamp_influxdb(timestamp):
    client = get_influxdb_client()
    if not client:
        print("Failed to get InfluxDB client")
        return False

    try:
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

        print(f"Successfully wrote point to InfluxDB: {point.to_line_protocol()}")
        client.close()
        return True
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
        print(f"Bucket: {os.environ['INFLUXDB_BUCKET']}")
        print(f"Org: {os.environ['INFLUXDB_ORG']}")
        print(f"Timestamp: {timestamp}")
        return False

app = FastAPI()

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
