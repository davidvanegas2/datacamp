import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def check_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Check all data for errors and transform it to the correct format.
    Args:
        data: The input data frame to check
    Returns:
        pd.DataFrame: The input data frame with rows removed
    """
    data = check_passenger_count(data)
    data = check_trip_distance(data)
    data = string_datetime_to_date(data)
    data = camel_case_to_snake_case(data)

    return data


def check_passenger_count(data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with passenger_count <= 0
    Args:
        data: The input data frame to check
    Returns:
        pd.DataFrame: The input data frame with rows removed
    """
    data = data[data.passenger_count > 0]
    return data


def check_trip_distance(data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with trip_distance <= 0
    Args:
        data: The input data frame to check
    Returns:
        pd.DataFrame: The input data frame with rows removed
    """
    data = data[data.trip_distance > 0]
    return data


def string_datetime_to_date(data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert string datetime to date
    Args:
        data: The input data frame to check
    Returns:
        pd.DataFrame: The input data frame with datetime converted to date
    """
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.date
    data['lpep_dropoff_date'] = pd.to_datetime(data['lpep_dropoff_datetime']).dt.date
    return data


def camel_case_to_snake_case(data: pd.DataFrame) -> pd.DataFrame:
    """
    Convert camel case column names to snake case
    Args:
        data: The input data frame to check
    Returns:
        pd.DataFrame: The input data frame with column names converted to snake case
    """
    data.columns = [re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col).lower() for col in data.columns]
    return data


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    data = check_data(data)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


@test
def test_column_names(output, *args) -> None:
    """
    Check that the column names are correct.
    """
    assert "vendor_id" in output.columns, "The column 'vendor_id' is missing"


@test
def test_passenger_count(output, *args) -> None:
    """
    Check that there are no rows with passenger_count <= 0
    """
    assert output[output.passenger_count <= 0].shape[0] == 0, "There are rows with passenger_count <= 0"


@test
def test_trip_distance(output, *args) -> None:
    """
    Check that there are no rows with trip_distance <= 0
    """
    assert output[output.trip_distance <= 0].shape[0] == 0, "There are rows with trip_distance <= 0"
