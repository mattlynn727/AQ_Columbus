Columbus Air Quality Data Collection and Prediction

This project aims to collect and analyze various environmental data related to air quality in Columbus, Ohio. It fetches data from multiple sources, processes it, and stores it in a centralized location for machine learning applications.

Scripts

AirNow.py:

Fetches current air quality data for Columbus, Ohio from the AirNow API.
Stores the air quality data and the maximum AQI value in Google Cloud Storage.

Air Now Historical Pull.py:

Retrieves historical air quality data for a specified date range from the AirNow API.

Energy Current.py:

Fetches energy generation data from the EIA API for the PJM region.
Stores the energy data in Google Cloud Storage.

Energy Historical.py:

Fetches historical energy data for a specific period from the EIA API.

LSTM.py:

Loads the master dataset from Google Cloud Storage.
Trains an LSTM (Long Short-Term Memory) neural network model to predict future air quality (AQI).
Saves the model's predictions to Google Cloud Storage.

TrafficCurrent.py:

Retrieves traffic flow data for specific highway segments in Columbus using the TomTom API.
Stores the collected traffic data in Google Cloud Storage.

WeatherCurrentPull.py:

Collects current weather data for Columbus, Ohio using the Weatherstack API.
Stores the data in Google Cloud Storage, appending to an existing file or creating a new one.

WeatherHistory.py:

Fetches historical weather data within a specified date range from the Weatherstack API

WildfireCurrent.py:

Fetches wildfire data from NASA's FIRMS API for the previous day.
Stores the raw data and a summarized version (binned by country) in Google Cloud Storage.

Feature Engineering.py:

Combines data from the various sources (traffic, weather, wildfire, energy, air quality) into a master dataset.
Performs data cleaning, preprocessing, and feature engineering.
Uploads the master dataset to Google Cloud Storage.

Project Purpose

The core purpose of this project is to empower individuals in Columbus, Ohio to make informed decisions regarding air quality. By developing an accurate air quality forecasting model and providing accessible information, the project strives to:

Inform the Public: Offer the general public real-time and predicted air quality data, enabling them to plan their daily activities, especially those sensitive to air pollution.

Offer a Second Opinion: Serve as an independent source of air quality information, allowing individuals to compare and verify official reports.

Promote Informed Decision-Making: Empower residents to take proactive steps to protect their health and well-being based on current and anticipated air quality conditions.
