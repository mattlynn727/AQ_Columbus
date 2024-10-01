import schedule
import datetime
import time
import requests
import pandas as pd
from google.cloud import storage
import os
import tempfile

# AirNow API configuration (replace with your actual key)
AIRNOW_API_KEY = 'YOUR_AIRNOW_API_KEY'
ZIP_CODE = "43215"
DISTANCE = 15  # Search radius in miles

# Google Cloud Storage configuration (replace with your actual service account key file path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH_TO_YOUR_SERVICE_ACCOUNT_KEY_FILE.json'
STORAGE_BUCKET_NAME = 'YOUR_STORAGE_BUCKET_NAME'

# File names within the storage bucket
ALL_AIR_QUALITY_DATA_FILENAME = 'air_quality_data_all.csv'
CURRENT_AQI_FILENAME = 'current-aqi.csv'

def get_and_save_air_quality_data():
    """
    Fetches current air quality data and saves it to cloud storage.
    """

    try:
        # Get today's date in EST timezone
        est_timezone = datetime.timezone(datetime.timedelta(hours=-4))
        today = datetime.datetime.now(tz=est_timezone).strftime('%Y-%m-%d')

        # Build the web address to get air quality data
        url = f"https://www.airnowapi.org/aq/observation/zipCode/current/?format=application/json&zipCode={ZIP_CODE}&distance={DISTANCE}&API_KEY={AIRNOW_API_KEY}"

        # Get the air quality data
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        if data:
            # Organize the air quality data into a table
            air_quality_data = pd.DataFrame([{
                'date': today,
                'location': item['ReportingArea'],
                'parameter_name': item['ParameterName'],
                'aqi': item['AQI'],
                'category': item['Category']['Name']
            } for item in data])

            # Convert 'date' column to month/day/year format
            air_quality_data['date'] = pd.to_datetime(air_quality_data['date']).dt.strftime('%m/%-d/%Y')

            # Connect to cloud storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(STORAGE_BUCKET_NAME)

            # Save all air quality data to cloud storage
            all_data_blob = bucket.blob(ALL_AIR_QUALITY_DATA_FILENAME)
            if all_data_blob.exists():
                # Add new data to existing file
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    all_data_blob.download_to_filename(temp_file.name)
                    existing_data = pd.read_csv(temp_file.name)
                updated_data = pd.concat([existing_data, air_quality_data], ignore_index=True)
                all_data_blob.upload_from_string(updated_data.to_csv(index=False), content_type='text/csv')
                os.remove(temp_file.name)
            else:
                # Create a new file for air quality data
                all_data_blob.upload_from_string(air_quality_data.to_csv(index=False), content_type='text/csv')

            # Save the highest AQI value
            max_aqi_data = pd.DataFrame({'Current AQI': [air_quality_data['aqi'].max()]})

            # Overwrite the 'current-aqi' file with the highest AQI
            current_aqi_blob = bucket.blob(CURRENT_AQI_FILENAME)
            current_aqi_blob.upload_from_string(max_aqi_data.to_csv(index=False), content_type='text/csv')

            print(f"Air quality data fetched and saved successfully!")
            print(f"Maximum AQI exported and 'current-aqi' dataset overwritten successfully!")

        else:
            print("No air quality data available for this location and date.")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")


# Schedule the task to run every hour
schedule.every().hour.do(get_and_save_air_quality_data)

# Keep the script running to check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)