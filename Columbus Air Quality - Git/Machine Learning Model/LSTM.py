import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.regularizers import l2
from sklearn.metrics import mean_squared_error
from google.cloud import storage
import os
import matplotlib.pyplot as plt
# from sklearn.preprocessing import RobustScaler
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.feature_selection import RFE
import schedule
import datetime
import time
import gc
from sklearn.model_selection import TimeSeriesSplit

# Set Google Cloud credentials path
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'XXXXXXXX'

storage_client = storage.Client()
master_dataset_bucket_name = 'master-aqi-bucket'
master_dataset_bucket = storage_client.bucket(master_dataset_bucket_name)

# Specify the forecast bucket name
forecast_dataset_bucket_name = 'columbus-forecast-bucket'
forecast_dataset_bucket = storage_client.bucket(forecast_dataset_bucket_name)

def run_LSTM():
    try:
        # 1. Load and preprocess the data
        blob = master_dataset_bucket.blob('master_dataset.csv')
        blob.download_to_filename('/tmp/master_dataset.csv')
        df = pd.read_csv('/tmp/master_dataset.csv')

        # Set 'Date' as the index and convert to datetime
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
        df.set_index('Date', inplace=True)

        # One-hot encode 'wind_dir' on the TRAINING data, preserving all categories
        df = pd.get_dummies(df, columns=['wind_dir'], prefix='wind_dir')

        # Get the list of all one-hot encoded wind direction columns from the training data
        all_wind_dir_columns = [col for col in df.columns if col.startswith('wind_dir_')]

        # Initial set of features (including one-hot encoded wind directions)
        all_features = ['traffic','temperature', 'humidity', 'wind_speed', 'pressure', 'precip', 'visibility',
                        'Canada', 'Central America', 'USA', 'Coal', 'Natural Gas', 'Other', 'Petroleum',
                        'Lagged_MaxAQI'] + \
                       all_wind_dir_columns

        target = ['MaxAQI']

        # Fill NaN values in the one-hot encoded wind direction columns with 0s
        df[all_wind_dir_columns] = df[all_wind_dir_columns].fillna(0)

        # Handle missing values (using ffill for the main DataFrame)
        df.ffill(inplace=True)

        # Directly use the provided list of selected features
        features = ['traffic','temperature', 'humidity', 'wind_speed', 'pressure', 'precip', 'visibility', 'Canada',
                    'Central America', 'USA', 'Other', 'Petroleum', 'Lagged_MaxAQI']

        # 2. Prepare sequences for LSTM
        def create_sequences(dataset, lookback):
            X, y = [], []
            for i in range(len(dataset) - lookback - 1):
                X.append(dataset[i:(i + lookback), :])
                y.append(dataset[i + lookback, -1])

            return np.array(X), np.array(y)

        lookback = 4
        X, y = create_sequences(df[features].values, lookback)

        # 3. Build and train multiple LSTM models (Ensemble) with TimeSeriesSplit
        n_models = 1
        ensemble_predictions = []
        batch_size = 64
        histories = []
        tscv = TimeSeriesSplit(n_splits=5)

        for i, (train_index, test_index) in enumerate(tscv.split(X)):
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]

            for i in range(n_models):
                model = Sequential()
                model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
                model.add(Dropout(0.2))
                model.add(LSTM(50 + i * 10, return_sequences=True))
                model.add(Dropout(0.2))
                model.add(LSTM(50 + i * 10, return_sequences=True))
                model.add(LSTM(50 + i * 10, return_sequences=True))
                model.add(LSTM(50 + i * 10))
                model.add(Dense(1))

                # Compile the model after defining its architecture with a new optimizer each time
                model.compile(loss='mean_squared_error', optimizer=tf.keras.optimizers.Adam())

                model.build(input_shape=(batch_size, lookback, len(features)))

                # Add early stopping
                early_stop = EarlyStopping(monitor='val_loss', patience=25)

                history = model.fit(X_train, y_train, epochs=80, batch_size=batch_size, validation_data=(X_test, y_test),
                                    callbacks=[early_stop])
                histories.append(history)

                # 5. Make predictions for the next 3 days (for each model)

                # Prepare the input data
                # Re-download the master dataset to get the latest data
                blob = master_dataset_bucket.blob('master_dataset.csv')
                blob.download_to_filename('/tmp/latest_master_dataset.csv')

                # Load the latest data
                latest_data = pd.read_csv('/tmp/latest_master_dataset.csv')

                # Preprocess the latest data
                latest_data['Date'] = pd.to_datetime(latest_data['Date'], format='%m/%d/%Y')
                latest_data.set_index('Date', inplace=True)

                # One-hot encode 'wind_dir' on the LATEST data
                latest_data = pd.get_dummies(latest_data, columns=['wind_dir'], prefix='wind_dir')

                # Align the columns
                for col in all_wind_dir_columns:
                    if col not in latest_data.columns:
                        latest_data[col] = 0
                latest_data = latest_data[
                    all_wind_dir_columns + [col for col in features if col not in all_wind_dir_columns]]

                # Fill missing values using ffill()
                latest_data.ffill(inplace=True)

                # After applying ffill to the latest_data
                pd.set_option('display.max_columns', None)
                print(latest_data.tail(3))

                # Create sequences for prediction
                last_sequence = latest_data[features].values[-lookback:]

                prediction_sequences = []
                for k in range(3):
                    next_pred = model.predict(last_sequence.reshape(1, lookback, len(features)))
                    prediction_sequences.append(next_pred[0, 0])

                    next_pred_reshaped = next_pred.reshape(-1)

                    # Update the last sequence for the next prediction
                    last_sequence = np.vstack([last_sequence[1:], np.hstack([next_pred_reshaped, last_sequence[-1, 1:]])])

                predictions = np.array(prediction_sequences)

                ensemble_predictions.append(predictions)

            # Average the predictions from all models in the ensemble
            final_predictions = np.mean(ensemble_predictions, axis=0)

            # Print the predictions
            future_dates = pd.date_range(start=df.index[-1] + pd.Timedelta(days=1), periods=3)
            for date, pred in zip(future_dates, final_predictions):
                print(f'Predicted AQI for {date.strftime("%m/%d/%Y")}: {pred}')

        # Create a DataFrame for predictions (outside the cross-validation loop)
        predictions_df = pd.DataFrame({'Date': future_dates, 'Predicted AQI': final_predictions})

        blob = forecast_dataset_bucket.blob('aqi_forecast_test.csv')
        blob.upload_from_string(predictions_df.to_csv(index=False), content_type='text/csv')
        print("Predictions saved to aqi_forecast.csv in columbus-forecast-bucket")

        # Print predictions
        print("Predictions:", final_predictions)

        # Calculate MSE and RMSE using the last fold's test set
        mse_original_scale = mean_squared_error(y_test[-3:], final_predictions)
        print("MSE:", mse_original_scale)
        print("RMSE:", np.sqrt(mse_original_scale))

    except Exception as e:
        print(f"An error occurred: {e}")

# Schedule the task to run every hour
run_LSTM()

# Keep the script running to execute scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)
