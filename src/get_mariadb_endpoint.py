import requests
import json
import statistics
from datetime import datetime, timedelta

# Define the base URL of your API
base_url = "http://host.docker.internal:8000"

# Define the endpoints and corresponding output files
endpoints = {
    "maria_create": "maria_create_med.json",
    "maria_delete": "maria_delete_med.json",
    "maria_simple_query": "maria_simple_query_med.json",
    "maria_join_query": "maria_join_query_med.json",
    "maria_all_query": "maria_all_query_med.json",
    "maria_update": "maria_update_med.json",
}

# Function to clean and parse the response
def clean_response(response_text):
    cleaned_text = response_text.replace('\\', '').strip('"')
    return json.loads(cleaned_text)

# Function to convert duration to milliseconds
def duration_to_milliseconds(duration_str):
    time_parts = duration_str.split(":")
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds, microseconds = map(float, time_parts[2].split("."))
    total_milliseconds = (hours * 3600 + minutes * 60 + seconds) * 1000 + microseconds / 1000
    return total_milliseconds

# Function to calculate the median duration for each span
def calculate_median_durations(data):
    span_durations = {}
    for entry in data:
        span = entry['span']
        duration = duration_to_milliseconds(entry['duration'])
        if span not in span_durations:
            span_durations[span] = []
        span_durations[span].append(duration)
    
    median_durations = []
    for span, durations in span_durations.items():
        median_duration = statistics.median(durations)
        median_durations.append({"span": span, "duration": f"{median_duration:.2f} ms"})
    
    return median_durations

# Loop through each endpoint and save the response to a file
for endpoint, output_file in endpoints.items():
    all_responses = []
    for _ in range(10):
        url = f"{base_url}/{endpoint}"
        try:
            # Call the endpoint and get the response
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Clean and parse the response
            cleaned_response = clean_response(response.text)
            all_responses.extend(cleaned_response)
        except requests.exceptions.RequestException as e:
            print(f"Error calling {endpoint}: {e}")

    # Calculate the median durations
    median_durations = calculate_median_durations(all_responses)

    # Save the median durations to a file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(median_durations, f, ensure_ascii=False, indent=4)

    print(f"Saved median durations from {endpoint} to {output_file}")
