import requests
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from google.cloud import storage
import os
import tempfile
import schedule
import time

# NASA FIRMS API configuration (replace with your actual key)
NASA_FIRMS_API_KEY = 'YOUR_NASA_FIRMS_API_KEY'
DATA_SOURCE = 'MODIS_NRT'
AREA_COORDINATES = '-140.8,12.7,-50.9,69.9'  # North and Central America

# Google Cloud Storage configuration
# (replace with your actual service account key file path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH_TO_YOUR_SERVICE_ACCOUNT_KEY_FILE.json'
STORAGE_BUCKET_NAME = 'YOUR_STORAGE_BUCKET_NAME'

# File names within the storage bucket
ALL_WILDFIRE_DATA_FILENAME = 'wildfire_data_all.csv'
BINNED_WILDFIRE_DATA_FILENAME = 'wildfire_data_binned.csv'

def get_wildfire_data_and_store():
    """
    Gets wildfire data from yesterday, saves it to cloud storage,
    and organizes the data by country and date.
    """

    today = datetime.today()
    yesterday = today - timedelta(days=1)

    # Build the web address to get wildfire data
    api_endpoint = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_FIRMS_API_KEY}/{DATA_SOURCE}/{AREA_COORDINATES}/1/{yesterday.strftime('%Y-%m-%d')}"

    try:
        # Get wildfire data
        response = requests.get(api_endpoint)
        response.raise_for_status()

        # Organize the data into a table
        wildfire_data = pd.read_csv(StringIO(response.text))

        # Figure out which country each wildfire is in
        def categorize_country(latitude):
            if latitude > 49:
                return 'Canada'
            elif latitude > 29:
                return 'USA'
            else:
                return 'Central America'

        wildfire_data['Country'] = wildfire_data['latitude'].apply(categorize_country)

        # Summarize fire intensity by country for yesterday
        yesterday_summary = wildfire_data.groupby('Country')['frp'].sum().reset_index()
        yesterday_summary['Date'] = yesterday.strftime('%Y-%m-%d')

        # Change date format to month-day-year
        yesterday_summary['Date'] = pd.to_datetime(yesterday_summary['Date']).dt.strftime('%m-%d-%Y')

        # Connect to cloud storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(STORAGE_BUCKET_NAME)

        # Handle organized (binned) data storage and updates
        binned_data_blob = bucket.blob(BINNED_WILDFIRE_DATA_FILENAME)
        if binned_data_blob.exists():
            # Add to existing organized data
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                binned_data_blob.download_to_filename(temp_file.name)
                existing_summary = pd.read_csv(temp_file.name)

                # Make sure all dates are in the same format
                if 'Date' in existing_summary.columns:
                    for i, row in existing_summary.iterrows():
                        if row['Date']:
                            try:
                                date_obj = pd.to_datetime(row['Date'], format='%Y-%m-%d')
                            except ValueError:
                                pass
                            else:
                                if pd.notnull(date_obj):
                                    existing_summary.at[i, 'Date'] = date_obj.strftime('%m-%d-%Y')

            updated_summary = pd.concat([existing_summary, yesterday_summary], ignore_index=True)

            # Make sure all dates use slashes instead of dashes
            updated_summary['Date'] = updated_summary['Date'].astype(str).str.replace('-', '/')
            binned_data_blob.upload_from_string(updated_summary.to_csv(index=False), content_type='text/csv')
            os.remove(temp_file.name)
        else:
            # Create a new file for organized data
            yesterday_summary['Date'] = yesterday_summary['Date'].astype(str).str.replace('-', '/')
            binned_data_blob.upload_from_string(yesterday_summary.to_csv(index=False), content_type='text/csv')

        # Save all the new wildfire data, replacing the old data
        all_data_blob = bucket.blob(ALL_WILDFIRE_DATA_FILENAME)
        all_data_blob.upload_from_string(wildfire_data.to_csv(index=False), content_type='text/csv')

        print(f"Wildfire data for {yesterday.strftime('%Y-%m-%d')} fetched, saved, and organized successfully!")

    except requests.exceptions.RequestException as e:
        print(f"Error getting wildfire data: {e}")

# Run this script every day at 7:58 PM
schedule.every().day.at("19:58").do(get_wildfire_data_and_store)

# Keep the script running to check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)