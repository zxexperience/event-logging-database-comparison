import json
import matplotlib.pyplot as plt
import pandas as pd

# List of JSON files
json_files = ['maria_simple_query_med.json', 'maria_create_med.json', 'maria_delete_med.json', 'maria_join_query_med.json', 'maria_all_query_med.json', 'maria_update_med.json']

# Loop through each JSON file and plot the data
for json_file in json_files:
    # Load JSON data with utf-8-sig encoding
    with open(json_file, encoding='utf-8-sig') as f:
        data = json.load(f)

    # Convert JSON data to a DataFrame
    df = pd.DataFrame(data)

    # Remove "ms" and convert 'duration' to float
    df['duration'] = df['duration'].str.replace(' ms', '').astype(float)

    # Sort the DataFrame by 'span'
    df = df.sort_values(by='span')

    # Create a new figure for each JSON file
    plt.figure(figsize=(12, 6))

    # Plot the data as points without lines
    plt.scatter(df['span'], df['duration'], marker='o')
    plt.xlabel('Span')
    plt.ylabel('Duration (ms)')
    plt.title(f'Span vs Duration - {json_file}')
    plt.grid(True)

    # Set x-ticks to show numbers every 50,000
    plt.xticks(range(0, max(df['span']) + 1, 50000), rotation=45)

    # Adjust layout to ensure the lower description is fully visible
    plt.tight_layout(pad=3.0)

    # Save the figure as a separate image file
    plt.savefig(f'{json_file.split(".")[0]}.png')

    # Show the plot
    plt.show()
