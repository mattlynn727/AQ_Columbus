import schedule
import datetime
import time
import requests
import pandas as pd
from google.cloud import storage
import os
import tempfile
import csv

# Weatherstack API configuration (replace with your actual key)
WEATHERSTACK_API_KEY = 'YOUR_WEATHERSTACK_API_KEY'

# Location for weather data
LOCATION = "Columbus, Ohio"

# Google Cloud Storage configuration
# (replace with your actual service account key file path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH_TO_YOUR_SERVICE_ACCOUNT_KEY_FILE.json'
STORAGE_BUCKET_NAME = 'YOUR_STORAGE_BUCKET_NAME'

# File name within the storage bucket
ALL_WEATHER_DATA_FILENAME = 'weather_data_all.csv'

def get_and_save_weather_data():
    """
    Gets current weather data and saves it to cloud storage.
    """
    try:
        # Build the web address to get weather data
        base_url = "http://api.weatherstack.com/current"
        url = f"{base_url}?access_key={WEATHERSTACK_API_KEY}&query={LOCATION}"

        # Get weather data
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        if data and data['current']:
            # Organize the weather data into a table
            weather_data = pd.DataFrame([{
                'date': pd.Timestamp.now().strftime('%-m/%-d/%Y'),  # Date in month/day/year format
                'temperature': data['current']['temperature'],
                'description': data['current']['weather_descriptions'],
                'humidity': data['current']['humidity'],
                'wind_speed': data['current']['wind_speed'],
                'wind_dir': data['current']['wind_dir'],
                'pressure': data['current']['pressure'],
                'precip': data['current']['precip'],
                'cloudcover': data['current']['cloudcover'],
                'feelslike': data['current']['feelslike'],
                'uv_index': data['current']['uv_index'],
                'visibility': data['current']['visibility']
            }])

            # Connect to cloud storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(STORAGE_BUCKET_NAME)

            # Get the file to store weather data
            weather_data_blob = bucket.blob(ALL_WEATHER_DATA_FILENAME)

            if weather_data_blob.exists():
                # Add new weather data to the existing file
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    weather_data_blob.download_to_filename(temp_file.name)

                    with open(temp_file.name, 'r') as csvfile:
                        reader = csv.reader(csvfile)
                        rows = list(reader)

                    # Make sure all dates are in the same format
                    if rows and len(rows[0]) > 0 and rows[0][0] == 'date':
                        for row in rows[1:]:
                            if row:
                                try:
                                    # Try to understand the date format and convert it if needed
                                    pd.to_datetime(row[0], format='%Y-%m-%d')
                                    row[0] = pd.to_datetime(row[0], format='%Y-%m-%d').strftime('%-m/%-d/%Y')
                                except ValueError:
                                    try:
                                        pd.to_datetime(row[0], format='%m-%d-%Y')
                                        row[0] = pd.to_datetime(row[0], format='%m-%d-%Y').strftime('%-m/%-d/%Y')
                                    except ValueError:
                                        pass

                    existing_data = pd.DataFrame(rows[1:], columns=rows[0])
                updated_data = pd.concat([existing_data, weather_data], ignore_index=True)
                weather_data_blob.upload_from_string(updated_data.to_csv(index=False), content_type='text/csv')
                os.remove(temp_file.name)

            else:
                # Create a new file to store weather data
                weather_data_blob.upload_from_string(weather_data.to_csv(index=False), content_type='text/csv')

            print(f"Weather data fetched and saved successfully!")

        else:
            print("No weather data available or unexpected API response.")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")

# Run this script twice a day, at 4 AM and 4 PM
schedule.every().day.at("04:00").do(get_and_save_weather_data)
schedule.every().day.at("16:00").do(get_and_save_weather_data)

# Keep the script running to check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)