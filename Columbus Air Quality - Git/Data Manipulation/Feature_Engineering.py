import pandas as pd
from google.cloud import storage
import os
import schedule
import time
import gc


def process_data():
    # Set your Google Cloud credentials path
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'XXXXXXXXXX'

    storage_client = storage.Client()

    # Dictionary mapping
    bucket_names = {
        'traffic_data_all_segments.csv': 'columbus-traffic-bucket',
        'weather_data_all.csv': 'columbus-weather-bucket',
        'wildfire_data_binned.csv': 'columbus-wildfire-bucket',
        'eia_data_all.csv': 'energy-generation-bucket',
        'air_quality_data_all.csv': 'columbus-aqi-bucket'
    }

    csv_files = list(bucket_names.keys())
    master_df = pd.DataFrame()

    for csv_file in csv_files:
        bucket_name = bucket_names[csv_file]
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(csv_file)

        # Download the CSV file to a temporary location
        temp_file_path = f'/tmp/{csv_file}'
        blob.download_to_filename(temp_file_path)

        df = pd.read_csv(temp_file_path)

        # Print column names for debugging
        print(f"Columns in {csv_file}: {df.columns}")

        if csv_file == 'traffic_data_all_segments.csv':
            # Standardize date
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y').dt.date

            df['SpeedDifference'] = df['freeFlowSpeed'] - df['currentSpeed']
            daily_speed_diff = df.groupby(['timestamp'])['SpeedDifference'].sum().reset_index()
            daily_speed_diff.columns = ['Date', 'TotalSpeedDifference']

            # Convert 'Date' to datetime before formatting
            daily_speed_diff['Date'] = pd.to_datetime(daily_speed_diff['Date'])

            # Format the 'Date' column in daily_speed_diff as 'MM/DD/YYYY'
            daily_speed_diff['Date'] = daily_speed_diff['Date'].dt.strftime('%m/%d/%Y')

            if master_df.empty:
                master_df = daily_speed_diff.copy()
            else:
                master_df = pd.merge(master_df, daily_speed_diff, on='Date', how='outer')


        elif csv_file == 'weather_data_all.csv':

            # Standardize date format
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
            columns_to_average = ['temperature', 'humidity', 'wind_speed', 'pressure', 'precip', 'visibility']
            daily_averages = df.groupby(df['date'].dt.date)[columns_to_average].mean().reset_index()
            daily_averages['wind_dir'] = df.groupby(df['date'].dt.date)['wind_dir'].first().reset_index()['wind_dir']

            # Rename 'date' column in daily_averages to match master_df
            daily_averages.rename(columns={'date': 'Date'}, inplace=True)

            # Convert 'Date' to datetime before formatting
            daily_averages['Date'] = pd.to_datetime(daily_averages['Date'])

            # Format the 'Date' column in daily_averages as 'MM/DD/YYYY'
            daily_averages['Date'] = daily_averages['Date'].dt.strftime('%m/%d/%Y')
            if master_df.empty:
                master_df = daily_averages.copy()
            else:
                master_df = pd.merge(master_df, daily_averages, on='Date', how='outer')





        elif csv_file == 'wildfire_data_binned.csv':
            # Standardize date format using the correct format '%m/%d/%Y'
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
            df = df.drop_duplicates(subset=['Date', 'Country'])

            # Pivot the DataFrame to have countries as columns
            df_pivoted = df.pivot(index='Date', columns='Country', values='frp').reset_index()

            # Format the 'Date' column in df_pivoted as 'MM/DD/YYYY'
            df_pivoted['Date'] = df_pivoted['Date'].dt.strftime('%m/%d/%Y')

            if master_df.empty:
                master_df = df_pivoted.copy()

            else:
                # Merge on the 'Date' column to ensure proper alignment
                master_df = pd.merge(master_df, df_pivoted, on='Date', how='outer')




        elif csv_file == 'eia_data_all.csv':
            # Specify the custom format (adjust as needed based on your data)
            df['period'] = pd.to_datetime(df['period'], format='%Y-%m-%dT%H-%M', utc=True)
            fuel_types_to_include = ['Coal', 'Natural Gas', 'Petroleum', 'Other']
            df_filtered = df[df['type-name'].isin(fuel_types_to_include)]

            # Group by the DATE part of the period and fuel type, then calculate the average
            daily_averages = df_filtered.groupby([df['period'].dt.date, 'type-name'])['value'].mean().reset_index()
            daily_averages.columns = ['Date', 'Fuel_Type', 'Average_Energy_Value']

            # Pivot the data to have fuel types as columns
            daily_averages_pivot = daily_averages.pivot(index='Date', columns='Fuel_Type',
                                                        values='Average_Energy_Value').reset_index()

            # Convert 'Date' to the same format as in master_df
            daily_averages_pivot['Date'] = pd.to_datetime(daily_averages_pivot['Date']).dt.strftime('%m/%d/%Y')
            if master_df.empty:
                master_df = daily_averages_pivot.copy()


            else:
                master_df = pd.merge(master_df, daily_averages_pivot, on='Date', how='outer')


        elif csv_file == 'air_quality_data_all.csv':
            # Standardize date format
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y', errors='coerce')

            # Calculate the maximum AQI per day
            daily_aqi_max = df.groupby(df['date'].dt.date)['aqi'].max().reset_index()
            daily_aqi_max.columns = ['Date', 'MaxAQI']

            # Convert 'Date' columns to datetime if they are not already
            daily_aqi_max['Date'] = pd.to_datetime(daily_aqi_max['Date'])

            # Calculate Lagged_MaxAQI (shift the MaxAQI by 1 day)
            daily_aqi_max['Lagged_MaxAQI'] = daily_aqi_max['MaxAQI'].shift(1)

            # The merge operation
            if master_df.empty:
                master_df = daily_aqi_max.copy()


            else:
                master_df['Date'] = pd.to_datetime(master_df['Date'], format='%m/%d/%Y')
                master_df = pd.merge(master_df, daily_aqi_max, on='Date', how='outer')

            # Reorder columns to have 'MaxAQI' last
            if master_df.columns[-1] != 'MaxAQI':  # Check if 'MaxAQI' is not already last
                master_df = master_df[[col for col in master_df.columns if col != 'MaxAQI'] + ['MaxAQI']]

            # Format the 'Date' columns in daily_aqi_data as 'MM/DD/YYYY'
            daily_aqi_max['Date'] = daily_aqi_max['Date'].dt.strftime('%m/%d/%Y')
            print(f"Shape of master_df after merging {csv_file}: {master_df.shape}")

        # Clean up the temporary file
        os.remove(temp_file_path)

    # Impute 0 for the specified columns
    columns_to_impute_zero = ['Canada', 'USA', 'Central America']

    # Get the index of the most recent row
    most_recent_row_index = master_df['Date'].idxmax()

    # Get the index of the row before the most recent one
    second_most_recent_row_index = master_df['Date'].sort_values(ascending=False).index[1]

    # Apply fillna(0) to all rows except the most recent and second most recent ones
    for col in columns_to_impute_zero:
        master_df.loc[~master_df.index.isin([most_recent_row_index, second_most_recent_row_index]), col] = \
            master_df.loc[~master_df.index.isin([most_recent_row_index, second_most_recent_row_index]), col].fillna(0)

    # Impute missing values with the mean of each column, except for the most recent row
    numerical_columns = master_df.select_dtypes(include=['number']).columns
    for col in numerical_columns:
        column_mean = master_df.loc[master_df.index != most_recent_row_index, col].mean()
        master_df.loc[master_df.index != most_recent_row_index, col] = master_df.loc[
            master_df.index != most_recent_row_index, col].fillna(column_mean)

    # Fill missing values in 'wind_dir' with the most frequent value, except for the most recent row
    most_frequent_wind_dir = master_df.loc[master_df.index != most_recent_row_index, 'wind_dir'].mode()[0]
    master_df.loc[master_df.index != most_recent_row_index, 'wind_dir'] = master_df.loc[
        master_df.index != most_recent_row_index, 'wind_dir'].fillna(most_frequent_wind_dir)

    # Upload the master dataset to Google Cloud Storage
    master_dataset_bucket_name = 'master-aqi-bucket'
    master_dataset_bucket = storage_client.bucket(master_dataset_bucket_name)

    # Format the 'Date' column in the final master_df as 'MM/DD/YYYY'
    master_df['Date'] = pd.to_datetime(master_df['Date']).dt.strftime('%m/%d/%Y')

    # Drop rows where 'Date' is NaN
    master_df.dropna(subset=['Date'], inplace=True)

    # Drop columns that are completely empty
    master_df.dropna(axis=1, how='all', inplace=True)

    try:
        master_dataset_bucket_name = 'master-aqi-bucket'
        master_dataset_bucket = storage_client.bucket(master_dataset_bucket_name)

        # Write the master dataframe to a CSV file
        master_df.to_csv('/tmp/master_dataset.csv', index=False)

        # Upload the CSV file to the bucket
        blob = master_dataset_bucket.blob('master_dataset.csv')
        blob.upload_from_filename('/tmp/master_dataset.csv')

        print('Master dataset uploaded successfully to Google Cloud Storage.')


# Schedule to run every hour
schedule.every().hour.do(process_data)

# Keep the script running to execute scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)