import requests
import json
import pandas as pd

def get_energy_data_from_eia(api_key):

    base_url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

    # Set up the request to get data for a specific time period
    headers = {
        "X-Params": json.dumps({
            "frequency": "local-hourly",
            "data": ["value"],
            "facets": {"respondent": ["PJM"]},
            "start": "2024-08-25T00-00:00",
            "end": "2024-08-31T23-00:00",
            "sort": [{"column": "period", "direction": "desc"}],
            "offset": 0,
            "length": 4050
        })
    }

    # Get the energy data
    response = requests.get(base_url, headers=headers, params={"api_key": api_key, "data[]": "value"})

    if response.status_code == 200:
        api_response = response.json()
        return api_response
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Replace with your actual EIA API key
EIA_API_KEY = "YOUR_EIA_API_KEY"

# Get the energy data
energy_data = get_energy_data_from_eia(EIA_API_KEY)

if energy_data:
    # Process the retrieved data
    if 'response' in energy_data and 'data' in energy_data['response']:
        data_list = energy_data['response']['data']

        # Organize the data into a table
        energy_data_table = pd.DataFrame(data_list)

        # Show the first few rows of the table
        print(energy_data_table.head().to_string(index=False))

        # Save the data to a CSV file
        energy_data_table.to_csv('Energy_Historical.csv', index=False)

    else:
        print("No data found in the response.")
else:
    print("No data retrieved from the API.")