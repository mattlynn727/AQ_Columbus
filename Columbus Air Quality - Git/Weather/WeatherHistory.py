import requests
import pandas as pd
import os
from datetime import datetime, timedelta

def get_past_weather_data(api_key, location, start_date, end_date, data_interval_hours=12):
    """
    Gets past weather data from the Weatherstack API for a specific time period.

    Args:
        api_key: Your Weatherstack API key.
        location: The place you want weather data for (e.g., "New York").
        start_date: The first day you want data for (YYYY-MM-DD format).
        end_date: The last day you want data for (YYYY-MM-DD format).
        data_interval_hours: How often you want data points (every 12 hours by default).

    Returns:
        A table containing the weather data, or None if there was an error.
    """

    base_url = "http://api.weatherstack.com/historical"

    params = {
        "access_key": api_key,
        "query": location,
        "historical_date_start": start_date,
        "historical_date_end": end_date,
        "hourly": 1,  # Get hourly data
        "interval": data_interval_hours
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        data = response.json()

        # Check if there's an error message from the API
        if 'error' in data:
            print(f"API Error: {data['error']['info']}")
            return None

        # Get the historical weather data
        if 'historical' in data and isinstance(data['historical'], dict):
            historical_data_list = list(data['historical'].values())
        else:
            print("Unexpected API response. 'historical' data not found or not in the right format.")
            return None

        all_date_dataframes = []

        for historical_date in historical_data_list:
            if not isinstance(historical_date, dict) or 'date' not in historical_date or 'hourly' not in historical_date:
                print(f"Skipping invalid data entry or missing 'hourly' data for date: {historical_date.get('date')}")
                continue

            date = historical_date['date']
            hourly_data = historical_date['hourly']

            df = pd.DataFrame(hourly_data)
            df['date'] = date

            all_date_dataframes.append(df)

        if all_date_dataframes:
            final_df = pd.concat(all_date_dataframes, ignore_index=True)
            return final_df
        else:
            print("No valid historical weather data with 'hourly' information found.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error getting weather data: {e}")
        return None

def get_weather_data_in_chunks(api_key, location, start_date, end_date, data_interval_hours=12, chunk_size_days=60):
    """
    Gets past weather data from the Weatherstack API in 60-day chunks.

    Args:
        api_key: Your Weatherstack API key.
        location: The place you want weather data for (e.g., "New York").
        start_date: The first day you want data for (YYYY-MM-DD format).
        end_date: The last day you want data for (YYYY-MM-DD format).
        data_interval_hours: How often you want data points (every 12 hours by default).
        chunk_size_days: The number of days in each chunk (60 days by default).

    Returns:
        A table containing the weather data for the entire period,
        or None if there was an error.
    """

    all_weather_data = []
    current_start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

    while current_start_date <= end_date_dt:
        # Calculate the end date for the current chunk
        current_end_date = min(current_start_date + timedelta(days=chunk_size_days - 1), end_date_dt)

        weather_data = get_past_weather_data(
            api_key, location,
            current_start_date.strftime('%Y-%m-%d'),
            current_end_date.strftime('%Y-%m-%d'),
            data_interval_hours
        )

        if weather_data is not None:
            all_weather_data.append(weather_data)

        current_start_date = current_end_date + timedelta(days=1)

    if all_weather_data:
        final_df = pd.concat(all_weather_data, ignore_index=True)
        return final_df
    else:
        print("No valid historical weather data found.")
        return None

# Example usage with a range of dates
API_KEY = 'YOUR_WEATHERSTACK_API_KEY'  # Replace with your actual API key
LOCATION = "Columbus"
START_DATE = "2024-09-01"
END_DATE = "2024-09-06"

weather_data = get_weather_data_in_chunks(API_KEY, LOCATION, START_DATE, END_DATE)

if weather_data is not None:
    # Specify where to save the CSV file
    csv_file_path = r"C:\Users\matly\PycharmProjects\Columbus Air Quality\Exported CSVs\historical_weather_data.csv"

    # Create the folder if it doesn't exist
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

    # Save the weather data to a CSV file
    weather_data.to_csv(csv_file_path, index=False)
    print(f"Weather data saved to {csv_file_path}")