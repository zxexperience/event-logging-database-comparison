import os
import random
import json
import logging
import string
import time
from fastapi import FastAPI
from datetime import datetime, timedelta
import mariadb
from influxdb_client import InfluxDBClient, Point # type: ignore
from influxdb_client.client.write_api import SYNCHRONOUS
import matplotlib.pyplot as plt

def get_mariadb_connection():
    try:
        connection = mariadb.connect(
            host=os.environ['MARIADB_HOST'],
            user=os.environ['MARIADB_USER'],
            password=os.environ['MARIADB_PASSWORD'],
            database=os.environ['MARIADB_DATABASE'],
            max_allowed_packet=1024 * 1024 * 256  # 64M
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

def get_db_connection():
    """
    Establishes a connection to the MariaDB database.

    Returns:
        conn: The database connection object.
        cursor: The database cursor object.
    """
    db_config = {
        'host': os.environ.get('MARIADB_HOST', 'localhost'),
        'port': int(os.environ.get('MARIADB_PORT', 3306)),
        'user': os.environ.get('MARIADB_USER', 'root'),
        'password': os.environ.get('MARIADB_PASSWORD', ''),
        'database': os.environ.get('MARIADB_DATABASE', 'test_db')
    }

    conn = mariadb.connect(**db_config)
    cursor = conn.cursor()
    return conn, cursor

def execute_query(query, params=None):
    """
    Executes a given query on the MariaDB database.

    Args:
        query (str): The SQL query to execute.
        params (tuple): The parameters to pass to the query.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    try:
        conn, cursor = get_db_connection()
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except mariadb.Error as e:
        print(f"Error executing query: {e}")
        return False

def fetch_query_results(query, params=None):
    """
    Fetches results from a given query on the MariaDB database.

    Args:
        query (str): The SQL query to execute.
        params (tuple): The parameters to pass to the query.

    Returns:
        list: A list of dictionaries containing the query results.
    """
    try:
        conn, cursor = get_db_connection()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except mariadb.Error as e:
        print(f"Error fetching query results: {e}")
        return []

def clear_events_table():
    """
    Clears all rows from the Event table in the MariaDB database.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    clear_query = "DELETE FROM Event"
    return execute_query(clear_query)

def insert_events_mariadb(events_data):
    """
    Inserts multiple events into the MariaDB database in chunks of 200,000 rows.

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
    insert_query = """
    INSERT INTO Event (timestamp, message, severity_ID, event_type_ID, source_ID)
    VALUES (%s, %s, %s, %s, %s)
    """

    chunk_size = 200000
    try:
        conn, cursor = get_db_connection()
        for i in range(0, len(events_data), chunk_size):
            chunk = events_data[i:i + chunk_size]
            values = [
                (
                    event_data['timestamp'],
                    event_data['message'],
                    event_data['severity_ID'],
                    event_data['event_type_ID'],
                    event_data['source_ID']
                )
                for event_data in chunk
            ]
            cursor.executemany(insert_query, values)
            conn.commit()
            print(f"Inserted {len(values)} rows into the Event table.")
        cursor.close()
        conn.close()
        return True
    except (mariadb.Error, ValueError) as e:
        print(f"Error inserting events into MariaDB: {e}")
        return False


def delete_events_mariadb(num_entries):
    """
    Deletes entries from the Event table in the MariaDB database.

    Args:
        num_entries (int): The number of entries to delete, starting from primary key 1.

    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    delete_query = """
    DELETE FROM Event
    WHERE id BETWEEN 1 AND %s
    """
    return execute_query(delete_query, (num_entries,))

def select_simple_events_mariadb():
    """
    Selects events from the MariaDB database based on specified criteria.

    Returns:
        list: A list of dictionaries, each containing the selected event data.
    """
    select_query_severity = """
    SELECT * FROM Event
    WHERE severity_ID = 2
    """
    results = fetch_query_results(select_query_severity)
    events_data = [
        {
            'timestamp': event[0],
            'message': event[1],
            'severity_ID': event[2],
            'event_type_ID': event[3],
            'source_ID': event[4]
        }
        for event in results
    ]
    return events_data

def select_join_events_mariadb():
    """
    Selects events from the MariaDB database based on specified criteria.

    Returns:
        list: A list of dictionaries, each containing the selected event data.
    """
    select_query_country = """
    SELECT e.* FROM Event e
    JOIN Source s ON e.source_ID = s.id
    JOIN Location l ON s.location_id = l.id
    WHERE l.country = 'Canada'
    """
    results = fetch_query_results(select_query_country)
    events_data = [
        {
            'timestamp': event[0],
            'message': event[1],
            'severity_ID': event[2],
            'event_type_ID': event[3],
            'source_ID': event[4]
        }
        for event in results
    ]
    return events_data

def update_simple_events_mariadb():
    """
    Selects events from the MariaDB database based on specified criteria.

    Returns:
        list: A list of dictionaries, each containing the selected event data.
    """
    select_query_severity = """
    UPDATE Event 
    SET severity_ID = 3 
    WHERE severity_ID = 2
    """
    try:
        results = fetch_query_results(select_query_severity)
        events_data = [
            {
                'timestamp': event[0],
                'message': event[1],
                'severity_ID': event[2],
                'event_type_ID': event[3],
                'source_ID': event[4]
            }
            for event in results
        ]
        return events_data
    except Exception as e:
        if "Cursor doesn't have a insert_result set" in str(e):
            logging.warning("No insert_result set found, but continuing execution.")
            return []
        else:
            logging.error(f"Error fetching query results: {e}")
            return None

SAMPLE_DATA = {
    "severities": [
        {"name": "INFO", "description": "Informational message"},
        {"name": "WARNING", "description": "Warning condition"},
        {"name": "ERROR", "description": "Error condition"},
        {"name": "CRITICAL", "description": "Critical condition"}
    ],
    "event_types": [
        {"name": "SYSTEM_STATUS", "description": "System status update"},
        {"name": "SECURITY_ALERT", "description": "Security-related event"},
        {"name": "PERFORMANCE", "description": "Performance metric event"},
        {"name": "USER_ACTION", "description": "User-initiated action"},
    ],
    "sources": [
        {
            "name": "web-server-01",
            "ip_address": "192.168.1.100",
            "location": {"name": "PL-01", "country": "Poland", "city": "Katowice"}
        },
        {
            "name": "web-server-02",
            "ip_address": "192.168.1.200",
            "location": {"name": "PL-02", "country": "Poland", "city": "Gdansk"}
        },
        {
            "name": "cache-01",
            "ip_address": "192.168.2.100",
            "location": {"name": "US-01", "country": "USA", "city": "New York"}
        },
        {
            "name": "lb-01",
            "ip_address": "192.168.3.100",
            "location": {"name": "DE-01", "country": "Germany", "city": "Frankfurt"}
        }
    ],
    "messages": {
        "SYSTEM_STATUS": [
            "System startup completed",
            "System shutdown initiated",
            "Service restart required",
            "Memory usage at {}%",
            "CPU utilization peaked at {}%"
        ],
        "SECURITY_ALERT": [
            "Failed login attempt from IP {}",
            "Suspicious activity detected",
            "Firewall rule updated",
            "New security patch applied",
            "User account locked after {} attempts"
        ],
        "PERFORMANCE": [
            "Response time exceeded {}ms",
            "Database query took {}ms",
            "Network latency increased to {}ms",
            "Queue size reached {}"
        ],
        "USER_ACTION": [
            "User {} logged in successfully",
            "Password change attempted",
            "Configuration updated by admin",
            "New user account created"
        ],
    }
}

def generate_data(events_to_generate):
    data = []
    for _ in range(events_to_generate):
        # Generate random timestamp with time variation (±12 hours)
        reference_time = datetime.now()
        time_variation = timedelta(hours=random.uniform(-12, 12))
        timestamp = (reference_time + time_variation).strftime("%Y-%m-%d %H:%M:%S")

        severity = random.choice(SAMPLE_DATA["severities"])
        event_type = random.choice(SAMPLE_DATA["event_types"])
        source = random.choice(SAMPLE_DATA["sources"])
        message_template = random.choice(SAMPLE_DATA["messages"][event_type["name"]])
        message = message_template.format(
            *[random.randint(1, 100) for _ in range(message_template.count("{}"))]
        )

        entry = {
            "timestamp": timestamp,
            "message": message,
            "severity_ID": SAMPLE_DATA["severities"].index(severity) + 1,
            "event_type_ID": SAMPLE_DATA["event_types"].index(event_type) + 1,
            "source_ID": SAMPLE_DATA["sources"].index(source) + 1
        }
        data.append(entry)
    return data

def process_data(operation, query_function=None):
    data = generate_data(100000)
    
    time_durations = []
    
    # Define spans for insert and other operations
    insert_spans = [1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]
    other_spans = [1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]
    
    if operation == "insert":
        for span in insert_spans:
            temp_data = data[:span]
            timestamp_start = datetime.now()
            insert_result = insert_events_mariadb(temp_data)
            if not insert_result:
                return json.dumps({'message': 'Error saving data in MariaDb!'})
            timestamp_end = datetime.now()
            duration = timestamp_end - timestamp_start
            time_durations.append({'span': span, 'duration': str(duration)})
    else:
        for span in other_spans:
            temp_data = data[:span]
            if operation == "delete":
                insert_result = insert_events_mariadb(temp_data)
                if not insert_result:
                    return json.dumps({'message': 'Error saving data in MariaDb!'})
                timestamp_start = datetime.now()
                operation_result = delete_events_mariadb(span)
                if not operation_result:
                    return json.dumps({'message': 'Error deleting data in MariaDb!'})
                timestamp_end = datetime.now()
            elif operation == "update":
                # Clear the table before inserting data
                clear_result = clear_events_table()
                if not clear_result:
                    return json.dumps({'message': 'Error clearing Event table in MariaDb!'})
                print(f"Table cleared for span {span}")
                insert_result = insert_events_mariadb(temp_data)
                if not insert_result:
                    return json.dumps({'message': 'Error saving data in MariaDb!'})
                timestamp_start = datetime.now()
                operation_result = update_simple_events_mariadb()
                print("????")
                if operation_result is None:
                    return json.dumps({'message': 'Error updating data in MariaDb!'})
                timestamp_end = datetime.now()
                time.sleep(10)
            elif operation == "query":
                # Clear the table before inserting data
                clear_result = clear_events_table()
                if not clear_result:
                    return json.dumps({'message': 'Error clearing Event table in MariaDb!'})
                print(f"Table cleared for span {span}")
                insert_result = insert_events_mariadb(temp_data)
                if not insert_result:
                    return json.dumps({'message': 'Error saving data in MariaDb!'})
                print(f"Data inserted for span {span}")
                timestamp_start = datetime.now()
                operation_result = query_function()
                if operation_result is None:
                    return json.dumps({'message': 'Error querying data in MariaDb!'})
                elif not operation_result:
                    print(f"No matching records found for span {span}")
                timestamp_end = datetime.now()
                print(f"Data queried for span {span}")
            
            duration = timestamp_end - timestamp_start
            time_durations.append({'span': span, 'duration': str(duration)})
    
    return json.dumps(time_durations)

logging.basicConfig(level=logging.INFO)

app = FastAPI()

#@app.on_event("startup")
#def startup_event():
#    data = import_data_from_file("./data_1000.json")
#    for record in data:
#       insert_result = insert_event_mariadb(record)
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

@app.get("/maria_create")
def maria_create():
    return process_data("insert")

@app.get("/maria_delete")
def maria_delete():
    return process_data("delete")

@app.get("/maria_simple_query")
def maria_simple_query():
    return process_data("query", select_simple_events_mariadb)

@app.get("/maria_update")
def maria_simple_update():
    return process_data("update")

@app.get("/maria_join_query")
def maria_join_query():
    return process_data("query", select_join_events_mariadb)
