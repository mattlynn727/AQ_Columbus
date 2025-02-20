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

        # One-hot encode 'wind_dir'
        df = pd.get_dummies(df, columns=['wind_dir'], prefix='wind_dir')
        all_wind_dir_columns = [col for col in df.columns if col.startswith('wind_dir_')]

        # Initial set of features (including one-hot encoded wind directions)
        all_features = ['traffic', 'temperature', 'humidity', 'wind_speed', 'pressure', 'precip', 'visibility',
                        'Canada', 'Central America', 'USA', 'Coal', 'Natural Gas', 'Other', 'Petroleum',
                        'Lagged_MaxAQI'] + all_wind_dir_columns

        target = ['MaxAQI']

        # Fill NaN values in the one-hot encoded wind direction columns with 0s
        df[all_wind_dir_columns] = df[all_wind_dir_columns].fillna(0)

        # Handle missing values (using ffill)
        df.ffill(inplace=True)

        # Selected Features
        features = ['traffic', 'temperature', 'humidity', 'wind_speed', 'pressure', 'precip', 'visibility', 'Canada',
                    'Central America', 'USA', 'Other', 'Petroleum', 'Lagged_MaxAQI'] + all_wind_dir_columns

        # 2. Prepare sequences for LSTM
        def create_sequences(dataset, lookback, target_index):
            X, y = [], []
            for i in range(len(dataset) - lookback - 1):
                X.append(dataset[i:(i + lookback), :])
                y.append(dataset[i + lookback, target_index])  # Use target_index
            return np.array(X), np.array(y)

        lookback = 4

        # Find the index of 'MaxAQI' for use in create_sequences
        target_index = df.columns.get_loc('MaxAQI')

        X, y = create_sequences(df[features + ['MaxAQI']].values, lookback, target_index) # Include MaxAQI in the input
        y = y.reshape(-1, 1)  # Ensure y is 2D


        # 3. Build and train multiple LSTM models (Ensemble) with TimeSeriesSplit
        n_models = 6
        ensemble_predictions = []
        batch_size = 64
        histories = []
        tscv = TimeSeriesSplit(n_splits=5)
        all_fold_predictions = [] # Store predictions for each fold
        all_fold_actuals = []

        for fold, (train_index, test_index) in enumerate(tscv.split(X)):
            print(f"Fold {fold+1}")
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]

            fold_predictions = [] # Predictions for THIS fold
            for i in range(n_models):
                model = Sequential()
                model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5), input_shape=(lookback, len(features))))
                model.add(Dropout(0.2))
                model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
                model.add(Dropout(0.2))
                model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
                model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
                model.add(LSTM(50 + i * 10, kernel_regularizer=l2(0.5)))
                model.add(Dense(1))

                # Compile the model
                model.compile(loss='mean_squared_error', optimizer=tf.keras.optimizers.Adam())

                # Add early stopping
                early_stop = EarlyStopping(monitor='val_loss', patience=25)

                history = model.fit(X_train, y_train, epochs=80, batch_size=batch_size, validation_data=(X_test, y_test),
                                    callbacks=[early_stop], verbose=0)
                histories.append(history)


                # Prediction for THIS fold
                last_sequence = X_test[-1].reshape(1, lookback, len(features)) # Use the LAST sequence of the TEST set
                model_preds = []
                for _ in range(3): # Predict 3 days
                    next_pred = model.predict(last_sequence, verbose=0)[0, 0]  # Predict, get single value
                    model_preds.append(next_pred)
                    # Update sequence:  Remove oldest, add new prediction, reshape
                    new_row = np.concatenate([last_sequence[0, 1:, :], [[next_pred] + [0] * (len(features) -1 )]], axis=0)  # Add prediction + padding
                    last_sequence = new_row.reshape(1, lookback, len(features))

                fold_predictions.append(model_preds)


            # Average predictions for the fold
            avg_fold_preds = np.mean(fold_predictions, axis=0)
            all_fold_predictions.append(avg_fold_preds)
            all_fold_actuals.append(y_test[-3:]) 

        # Combine Predictions and Actuals
        # Flatten the lists of predictions and actuals
        all_fold_predictions = np.concatenate(all_fold_predictions)
        all_fold_actuals = np.concatenate(all_fold_actuals)

        # Calculate overall MSE and RMSE
        mse_original_scale = mean_squared_error(all_fold_actuals, all_fold_predictions)
        print("Overall MSE:", mse_original_scale)
        print("Overall RMSE:", np.sqrt(mse_original_scale))

        # Final Prediction
        # After cross-validation, train on the ENTIRE dataset for the final prediction
        final_ensemble_predictions = []
        for i in range(n_models):
            final_model = Sequential()
            final_model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5), input_shape=(lookback, len(features))))
            final_model.add(Dropout(0.2))
            final_model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
            final_model.add(Dropout(0.2))
            final_model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
            final_model.add(LSTM(50 + i * 10, return_sequences=True, kernel_regularizer=l2(0.5)))
            final_model.add(LSTM(50 + i * 10, kernel_regularizer=l2(0.5)))
            final_model.add(Dense(1))
            final_model.compile(loss='mean_squared_error', optimizer=tf.keras.optimizers.Adam())
            final_model.fit(X, y, epochs=80, batch_size=batch_size, callbacks=[early_stop], verbose=0) # Use the whole dataset (X, y)

            # Prepare the input data for final prediction
            last_sequence_final = df[features].values[-lookback:].reshape(1, lookback, len(features))
            final_model_preds = []
            for _ in range(3):
                next_pred_final = final_model.predict(last_sequence_final, verbose=0)[0, 0]
                final_model_preds.append(next_pred_final)
                # Update the sequence
                new_row_final = np.concatenate([last_sequence_final[0, 1:, :], [[next_pred_final] + [0] * (len(features) - 1)]], axis=0) #padding
                last_sequence_final = new_row_final.reshape(1, lookback, len(features))

            final_ensemble_predictions.append(final_model_preds)

        final_predictions = np.mean(final_ensemble_predictions, axis=0)


        # Output and Save Predictions
        future_dates = pd.date_range(start=df.index[-1] + pd.Timedelta(days=1), periods=3)
        for date, pred in zip(future_dates, final_predictions):
            print(f'Predicted AQI for {date.strftime("%m/%d/%Y")}: {pred}')

        predictions_df = pd.DataFrame({'Date': future_dates, 'Predicted AQI': final_predictions})
        blob = forecast_dataset_bucket.blob('aqi_forecast_test.csv')
        blob.upload_from_string(predictions_df.to_csv(index=False), content_type='text/csv')
        print("Predictions saved to aqi_forecast.csv in columbus-forecast-bucket")

        # Print predictions
        print("Predictions:", final_predictions)



    except Exception as e:
        print(f"An error occurred: {e}")

# Schedule the task to run every hour
run_LSTM()

# Keep the script running to execute scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)
