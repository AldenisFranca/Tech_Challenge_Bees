"""test_transform.py

Tech Challenge - Data Engineer - AB-InBev | Bees
Candidato: Aldenis França

Objective:
    The goal of this test is to assess your skills in consuming data from an API, transforming and
    persisting it into a data lake following the medallion architecture with three layers: raw data,
    curated data partitioned by location, and an analytical aggregated layer.
"""

# Importing the Libraries
import pytest
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from chispa import assert_df_equality  # to compare DataFrames
from scripts.utils import get_logger
from scripts.transform import remove_identical_columns, check_plausible_longitude_latitude_values, fix_implausible_latitude_values


# Logging function call
logger = get_logger('Tests')


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.master('local[1]').appName('pytest-pyspark').getOrCreate()


def test_remove_identical_columns(spark):

    # DataFrame with columns with same values
    people_data = [
        (1, 'Alice', 85.0, -130.9, 'Street A', 85.0, 'Street A'),
        (2, 'Bob', 40.0, 120.0, 'Street B', 40.0, 'Street B'),
        (3, 'Carol', 1.3, 103.6, 'Street C', 1.3, 'Street C')
    ]
    people_df = spark.createDataFrame(
        people_data, ['id', 'name', 'latitude', 'longitude', 'address', 'length', 'street']
    )

    # Expected DataFrame
    expected_data_non_identical_columns = [
        (1, 'Alice', 85.0, -130.9, 'Street A'),
        (2, 'Bob', 40.0, 120.0, 'Street B'),
        (3, 'Carol', 1.3, 103.6, 'Street C')
    ]
    expected_df = spark.createDataFrame(
        expected_data_non_identical_columns, ['id', 'name', 'latitude', 'longitude', 'address']
    )

    # Run pipeline
    result_df = remove_identical_columns(people_df, 'address', 'street')
    result_df = remove_identical_columns(result_df, 'latitude', 'length')

    # Validates equality
    try:
        assert_df_equality(result_df, expected_df, ignore_row_order=True), 'DataFrames are not equal'
    except AssertionError as ae:
        logger.exception(f'Test error: {str(ae)}')
        raise


def test_check_plausible_longitude_latitude_values(spark):

    # DataFrame with invalid latitude and longitude
    people_data = [
        (1, 'Alice', 95.0, -30.9, 'Street A'),   # invalid latitude
        (2, 'Bob', 40.0, 200.0, 'Street B'),     # invalid longitude
        (3, 'Carol', 1.3, 103.6, 'Street C'),    # valid latitude and longitude
        (4, 'David', 90.0, -380.0, 'Street D')   # invalid longitude
    ]
    people_df = spark.createDataFrame(
        people_data, ['id', 'name', 'latitude', 'longitude', 'address']
    )

    # Expected return
    expected_return_lat_long = (2, 1, 1)

    # Run pipeline
    return_df = check_plausible_longitude_latitude_values(people_df, 'longitude', 'latitude')

    # Validates equality
    try:
        assert return_df == expected_return_lat_long, f'Expected: {expected_return_lat_long}, Got: {return_df}'
    except AssertionError as ae:
        logger.exception(f'Test error: {str(ae)}')
        raise


def test_fix_implausible_latitude_values(spark):

    # DataFrame with invalid latitude, but between +90 and +180 and longitude between -90 and +90
    people_data = [
        (1, 'Alice', 95.0, -30.9, 'Street A'),   # invalid latitude
        (2, 'Bob', 40.0, 120.0, 'Street B'),     # invalid longitude
        (3, 'Carol', 1.3, 103.6, 'Street C'),    # valid latitude and longitude
        (4, 'David', 90.0, -180.0, 'Street D')   # invalid longitude
    ]
    people_df = spark.createDataFrame(
        people_data, ['id', 'name', 'latitude', 'longitude', 'address']
    )

    # Expected DataFrame
    expected_data_with_fixed_columns = [
        (1, 'Alice', -30.9, 95.0, 'Street A'),
        (2, 'Bob', 40.0, 120.0, 'Street B'),
        (3, 'Carol', 1.3, 103.6, 'Street C'),
        (4, 'David', 90.0, 180.0, 'Street D')
    ]
    expected_df = spark.createDataFrame(
        expected_data_with_fixed_columns, ['id', 'name', 'latitude', 'longitude', 'address']
    )

    # Run pipeline
    result_df = fix_implausible_latitude_values(people_df, 'longitude', 'latitude')

    # Validates equality
    try:
        assert_df_equality(result_df, expected_df, ignore_row_order=True), 'DataFrames are not equal'
    except AssertionError as ae:
        logger.exception(f'Test error: {str(ae)}')
        raise