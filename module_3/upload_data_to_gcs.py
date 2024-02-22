"""
This file will get PARQUET files from an API from Green Taxi Data for each month of 2022
and upload them to a Google Cloud Storage bucket.

The data are available at the following URL:
- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Once the data is uploaded to GCS, it can be used in a BigQuery table to perform analysis.
"""
import os
import requests
import pandas as pd
from google.cloud import storage
from io import BytesIO

# Set the environment variable to authenticate with Google Cloud
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/dvanegas/Documents/GitHub/datacamp/gcp_credentials/de-course-411516-c64d64bd809f.json"

# Set the project ID
project_id = "de-course-411516"

# Set the bucket name
BUCKET_NAME = "terraform_bucket_dfvanegas"

YEAR = str(2019)
COLOR_TAXI = "yellow"

# Set the URL for the API
URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{COLOR_TAXI}_tripdata_{YEAR}-"

# Set the file extension
file_extension = ".parquet"


def get_str_month(month: int) -> str:
    """
    Returns the string representation of a month.

    Args:
        month: The month as an integer

    Returns:
        The month as a string
    """
    return str(month).zfill(2)


def generate_url(month: str) -> str:
    """
    Generates the URL for the API.

    Args:
        month: The month to get the data for

    Returns:
        The URL for the API
    """
    return URL + month + file_extension


def request_data_from_api(url: str) -> pd.DataFrame:
    """
    Requests data from an API and returns a pandas DataFrame.

    Args:
        url: The URL of the API

    Returns:
        A pandas DataFrame
    """
    # Get the data from the API
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Read the data into a pandas DataFrame
        df = pd.read_parquet(BytesIO(response.content))
        return df
    else:
        print(f"Failed to get data from {url}")
        return pd.DataFrame()


def upload_file_to_gcs(client: storage.Client, file_name: str):
    """
    Uploads a file to Google Cloud Storage.

    Args:
        client: The GCS client
        file_name: The name of the file to upload
    """
    # Get the bucket
    bucket = client.get_bucket(BUCKET_NAME)

    # Create a blob
    blob = bucket.blob(file_name)

    # Upload the file
    blob.upload_from_filename(file_name)
    print(f"File {file_name} uploaded to {BUCKET_NAME}")


def main():
    """
    Main function to get the data from the API and upload it to GCS
    :return:
    """
    # Create a GCS client
    client = storage.Client(project=project_id)

    # Get the data for each month of 2022
    for i in range(1, 13):
        month = get_str_month(i)
        # Generate the URL
        url = generate_url(month)

        # Get the data from the API
        df = request_data_from_api(url)

        # Check if the DataFrame is not empty
        if not df.empty:
            # Save the DataFrame to a PARQUET file
            file_name = f"data/{COLOR_TAXI}_tripdata_{YEAR}-{str(month).zfill(2)}{file_extension}"
            df.to_parquet(file_name)

            # Upload the file to GCS
            upload_file_to_gcs(client, file_name)

            # Delete the file
            os.remove(file_name)


if __name__ == "__main__":
    main()
