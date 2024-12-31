import pandas as pd
import requests
import time

API_KEY = '5b3ce3597851110001cf62483b7f8ce3024e411daaea548768eb8f03'
API_URL = 'https://api.openrouteservice.org/v2/directions/driving-car'

def get_travel_time(source_lat, source_lon, destination_lat, destination_lon):
    try:
        headers = {'Authorization': API_KEY, 'Content-Type': 'application/json'}
        payload = {
            "coordinates": [
                [source_lon, source_lat],
                [destination_lon, destination_lat]
            ]
        }
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        travel_time = data['routes'][0]['summary']['duration']  # Time in seconds
        return travel_time / 60  # Convert to minutes
    except Exception as e:
        print(f"Error: {e}")
        return None

# Load the CSV file
input_file = 'final_csv.csv'
output_file = 'final_csv_with_teeja_travel_time.csv'

# Load the CSV and resume if the file already exists
try:
    df = pd.read_csv(output_file)
    print("Resuming from existing file.")
except FileNotFoundError:
    df = pd.read_csv(input_file)
    df['Travel_time_minutes'] = None

# Count remaining rows to process
rows_processed = df['Travel_time_minutes'].notna().sum()
total_rows = len(df)
print(f"Rows already processed: {rows_processed}/{total_rows}")

# Process remaining rows
batch_size = 2500  # Number of rows to process per run
rows_to_process = batch_size

for index, row in df.iterrows():
    if rows_processed >= total_rows:
        break

    if pd.notna(row['Travel_time_minutes']):
        # Skip rows already processed
        continue

    source_lat = row['Restaurant_latitude']
    source_lon = row['Restaurant_longitude']
    destination_lat = row['Delivery_location_latitude']
    destination_lon = row['Delivery_location_longitude']

    travel_time = get_travel_time(source_lat, source_lon, destination_lat, destination_lon)
    df.at[index, 'Travel_time_minutes'] = travel_time

    rows_processed += 1
    print(f"Processed row {rows_processed}/{total_rows}")

    # To avoid hitting the rate limit, sleep between API calls
    time.sleep(1)

    # Save progress after every 100 rows
    if rows_processed % 100 == 0:
        df.to_csv(output_file, index=False)
        print(f"Progress saved at {rows_processed} rows.")

print("Processing complete!")
df.to_csv(output_file, index=False)
print(f"Updated file saved as {output_file}")
