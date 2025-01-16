# event-logging-db-comparison
MariaDB vs. InfluxDB Performance Comparison

# CHANGELOG 16-01-2024
- init.sql dla MariaDB zawiera  bazy danych
  - Event
    - timestamp TIMESTAMP
    - message VARCHAR (255)
    - severity_ID INT
    - event_type_ID INT
    - source_ID INT

  - Severity
    - name VARCHAR (63)
    - description VARCHAR (255)

  - Event_type
    - name VARCHAR (63)
    - description VARCHAR (255)

  - Source
    - name VARCHAR (63)
    - ip_address VARCHAR (45)
    - location_id INT

  - Location
    - name VARCHAR (63)
    - country VARCHAR (63)
    - city VARCHAR (63) 

- predefiniowane wartości - 150 sources, 50 Locations, 3 Severity, 6 Event_type
- testowe dane file data_100000.json
- zapis do MariaDB in bulk