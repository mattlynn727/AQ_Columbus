import schedule
import datetime
import time
import requests
import pandas as pd
from google.cloud import storage
import os
import json
import tempfile

# Google Cloud Storage configuration (replace with your actual service account key file path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH_TO_YOUR_SERVICE_ACCOUNT_KEY_FILE.json'
STORAGE_BUCKET_NAME = 'YOUR_STORAGE_BUCKET_NAME'

def get_and_save_energy_data():
    """
    Fetches energy data from the EIA API and saves it to cloud storage.
    """
    try:
        # EIA API key (replace with your actual key)
        EIA_API_KEY = "YOUR_EIA_API_KEY"

        base_url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

        # Get today's date
        today = datetime.date.today().strftime('%Y-%m-%d')

        # Set up the request to get today's data up to 11:00 PM
        headers = {
            "X-Params": json.dumps({
                "frequency": "local-hourly",
                "data": ["value"],
                "facets": {"respondent": ["PJM"]},
                "start": f"{today}T00-00:00",
                "end": f"{today}T23-00:00",
                "sort": [{"column": "period", "direction": "desc"}],
                "offset": 0,
                "length": 4050
            })
        }

        # Get the energy data
        response = requests.get(base_url, headers=headers, params={"api_key": EIA_API_KEY, "data[]": "value"})
        response.raise_for_status()

        data = response.json()

        if data and 'response' in data and 'data' in data['response']:
            data_list = data['response']['data']
            energy_data = pd.DataFrame(data_list)

            # Connect to cloud storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(STORAGE_BUCKET_NAME)

            # Save the energy data to cloud storage
            blob_name = "eia_data_all.csv"
            energy_data_blob = bucket.blob(blob_name)

            if energy_data_blob.exists():
                # Add new data to the existing file
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                    energy_data_blob.download_to_filename(temp_file.name)
                    existing_data = pd.read_csv(temp_file.name)
                updated_data = pd.concat([existing_data, energy_data], ignore_index=True)
                energy_data_blob.upload_from_string(updated_data.to_csv(index=False), content_type='text/csv')
                os.remove(temp_file.name)

            else:
                # Create a new file to store the energy data
                energy_data_blob.upload_from_string(energy_data.to_csv(index=False), content_type='text/csv')

            print(f"EIA data for {today} fetched and saved successfully!")
        else:
            print("No data found in the EIA response.")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")


# Schedule the task to run every day at 11:58 PM
schedule.every().day.at("23:58").do(get_and_save_energy_data)

# Keep the script running to check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)