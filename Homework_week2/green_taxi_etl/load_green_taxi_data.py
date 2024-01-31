import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"

    # Data types dictionary
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    # Initialize an empty list to store individual DataFrames
    dataframes = []

    # Load data for the final quarter using a for loop
    for month in range(10, 13):  # Months 10, 11, 12
        # Construct the file name for the current month
        file_name = f'green_tripdata_2020-{month:02d}.csv.gz'
        
        # Construct the full URL for the current file on GitHub
        file_url = f"{base_url}{file_name}"
        
        print(file_url)
        # Use requests to get the content of the URL
        response = requests.get(file_url)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Use pandas read_csv with dtype and parse_dates options
            current_data = pd.read_csv(
                io.BytesIO(response.content),
                compression='gzip',
                dtype=taxi_dtypes,
                parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
            )
            
            # Append the DataFrame to the list
            dataframes.append(current_data)
        else:
            print(f"Failed to retrieve data from the URL. Status code: {response.status_code}")

    # Concatenate the individual DataFrames into a single DataFrame
    final_quarter_data = pd.concat(dataframes, ignore_index=True)

    # Display or further process the combined DataFrame
    print(final_quarter_data.head())
    return final_quarter_data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

