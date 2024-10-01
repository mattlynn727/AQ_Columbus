import requests
import datetime
import pandas as pd
import time

# AirNow API configuration (replace with your actual key)
AIRNOW_API_KEY = 'YOUR_AIRNOW_API_KEY'
ZIP_CODE = "43215"
DISTANCE = 15

# Date range for historical data
START_DATE = datetime.datetime(year=2024, month=9, day=7)
END_DATE = datetime.datetime(year=2024, month=9, day=9)

# List to store the data for each day
all_data = []

# Go through each day in the date range
current_date = START_DATE
while current_date <= END_DATE:
    # Format the date for the API request
    date_str = current_date.strftime("%Y-%m-%dT00-0000")

    # Build the web address to get historical air quality data
    api_url = f"https://www.airnowapi.org/aq/observation/zipCode/historical/?format=application/json&zipCode={ZIP_CODE}&date={date_str}&distance={DISTANCE}&API_KEY={AIRNOW_API_KEY}"

    try:
        response = requests.get(api_url)

        # Check if we hit the API's rate limit
        if response.status_code == 429:
            # If so, wait for the specified time before trying again
            retry_after = int(response.headers.get('Retry-After', 60))
            print(f"Rate limit reached. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            continue

        response.raise_for_status()

        data = response.json()
        if isinstance(data, list):
            # If the data is in the expected format, add it to our list
            all_data.extend(data)
        else:
            print(f"Unexpected data format for {current_date}: {data}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred for {current_date}: {e}")

    # Move to the next day
    current_date += datetime.timedelta(days=1)

# If we got any data, organize it and save it
if all_data:
    final_df = pd.DataFrame(all_data)

    # Save the data to a CSV file
    final_df.to_csv('airnow_data_final.csv', index=False)

    print(final_df)
else:
    print("No data was retrieved for the specified date range.")