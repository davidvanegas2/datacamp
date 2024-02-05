import os

from datetime import datetime
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

YEAR = 2020
START_MONTH = 10
END_MONTH = 12
URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
DATA_PATH = '/home/src/mage_data/magic-zoomcamp/pipelines/green_taxi_etl'


def get_formatted_date(month: int) -> str:
    """
    Get the formatted date for the given month.
    Args:
        month: int representing the month (1-12)
    Returns:
        str: formatted date for the given month
    """
    date_obj = datetime(year=YEAR, month=month, day=1)
    formatted_date = date_obj.strftime("%Y-%m")
    return formatted_date


def get_url_to_csv_file(month: int) -> str:
    """
    Get the file name for the given month.
    Args:
        month: int representing the month (1-12)
    Returns:
        str: file name for the given month
    """
    formatted_date = get_formatted_date(month)
    csv_name = f'green_tripdata_{formatted_date}.csv.gz'
    url = f'{URL}/{csv_name}'
    return url


def download_data(url: str) -> str:
    """
    Download data from github.
    Args:
        url: str representing the url to the file
    Returns:
        str: path to the downloaded file
    """
    data_path = os.path.join(DATA_PATH, url.split('/')[-1])
    os.system(f"wget {url} -O {data_path}")
    print(f"Downloaded data to {data_path}")
    return data_path


def read_dataframe(data_path: str) -> pd.DataFrame:
    """
    Read the data from the given path.
    Args:
        data_path: str representing the path to the data
    Returns:
        pd.DataFrame: data frame with the data
    """
    df = pd.read_csv(data_path, compression='gzip')
    return df


def concat_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate the given data frames.
    Args:
        dfs: list of data frames
    Returns:
        pd.DataFrame: concatenated data frame
    """
    df = pd.concat(dfs)
    return df


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    all_dfs = []
    for month in range(START_MONTH, END_MONTH + 1):
        url = get_url_to_csv_file(month)
        data_path = download_data(url)
        current_df = read_dataframe(data_path)
        all_dfs.append(current_df)
    df = pd.concat(all_dfs)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
