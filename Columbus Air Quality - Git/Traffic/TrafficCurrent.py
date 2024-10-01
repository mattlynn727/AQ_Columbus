import schedule
import datetime
import time
import requests
import pandas as pd
from google.cloud import storage
import os
import tempfile

# TomTom API configuration (replace with your actual key)
TOMTOM_API_KEY = 'YOUR_TOMTOM_API_KEY'

# Highway segments we're interested in, with their locations
highway_segments = {
    "I-70 West Downtown": (39.973589, -83.082973),
    "I-70 East Downtown": (39.952439, -82.945859),
    "I-71 North Downtown": (40.007293, -82.985081),
    "I-71 South Downtown": (39.907918, -83.023019),
    "I-315 North Downtown": (39.998029, -83.026691),
    "I-670 West Downtown": (39.966024, -83.036566),
    "I-670 East Downtown": (39.978885, -82.970333),
}

# Google Cloud Storage configuration
# (replace with your actual service account key file path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH_TO_YOUR_SERVICE_ACCOUNT_KEY_FILE.json'
STORAGE_BUCKET_NAME = 'YOUR_STORAGE_BUCKET_NAME'

def get_and_save_traffic_data():
    """
    Gets traffic data for specific highway segments and saves it to cloud storage
    """

    try:
        # Create an empty table to store all the traffic data
        all_traffic_data = pd.DataFrame(columns=['timestamp', 'segment_name', 'frc', 'currentSpeed', 'freeFlowSpeed'])

        for segment_name, (center_lat, center_lon) in highway_segments.items():
            # Set how much area around the highway segment to look at
            zoom_level = 13
            radius_degrees = 0.02
            bbox = [
                center_lat - radius_degrees,
                center_lon - radius_degrees,
                center_lat + radius_degrees,
                center_lon + radius_degrees
            ]

            # Build the web address to get traffic data
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom_level}/json?key={TOMTOM_API_KEY}&bbox={bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}&point={center_lat},{center_lon}"

            # Get traffic data
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            # Get the current time in Eastern Daylight Time (EDT)
            edt_now = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=-4)))

            if isinstance(data, dict) and 'flowSegmentData' in data:
                flow_segment_data = data['flowSegmentData']

                # Figure out if it's still the same day or if it's technically the next day (between midnight and 4 AM EDT)
                if edt_now.hour >= 0 and edt_now.hour < 4:
                    today = (edt_now - datetime.timedelta(days=1)).strftime('%m/%d/%Y')
                else:
                    today = edt_now.strftime('%m/%d/%Y')

                # Organize the traffic data into a table
                traffic_data_for_segment = pd.DataFrame([{
                    'timestamp': today,
                    'segment_name': segment_name,
                    'frc': flow_segment_data['frc'],
                    'currentSpeed': flow_segment_data['currentSpeed'],
                    'freeFlowSpeed': flow_segment_data['freeFlowSpeed'],
                }])

                # Add this segment's data to the main table
                all_traffic_data = pd.concat([all_traffic_data, traffic_data_for_segment], ignore_index=True)

            else:
                print(f"API Error or Unexpected Response for {segment_name}: {data}")

        # Connect to cloud storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(STORAGE_BUCKET_NAME)

        # Save all the traffic data to cloud storage
        blob_name = 'traffic_data_all_segments.csv'
        traffic_data_blob = bucket.blob(blob_name)
        if traffic_data_blob.exists():
            # Add new data to the existing file
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
                traffic_data_blob.download_to_filename(temp_file.name)
                existing_data = pd.read_csv(temp_file.name)
            updated_data = pd.concat([existing_data, all_traffic_data], ignore_index=True)
            traffic_data_blob.upload_from_string(updated_data.to_csv(index=False), content_type='text/csv')
            os.remove(temp_file.name)
        else:
            # Create a new file to store the traffic data
            traffic_data_blob.upload_from_string(all_traffic_data.to_csv(index=False), content_type='text/csv')

        print(f"Traffic data for all segments fetched and saved successfully!")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")


# Schedule the task to run at 9 AM, 12 PM, 5 PM, and 9 PM every day
schedule.every().day.at("09:00").do(get_and_save_traffic_data)
schedule.every().day.at("12:00").do(get_and_save_traffic_data)
schedule.every().day.at("17:00").do(get_and_save_traffic_data)
schedule.every().day.at("21:00").do(get_and_save_traffic_data)

# Keep the script running to check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)