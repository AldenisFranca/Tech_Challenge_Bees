"""quality.py

Tech Challenge - Data Engineer - AB-InBev | Bees
Candidato: Aldenis França

Objective:
    The goal of this test is to assess your skills in consuming data from an API, transforming and
    persisting it into a data lake following the medallion architecture with three layers: raw data,
    curated data partitioned by location, and an analytical aggregated layer.
"""

# Importing the Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import get_logger, create_spark, read_parquet


"""Starting a PySpark Session"""

# Creating the Spark Session
logger = get_logger('Quality')
spark = create_spark('Data Quality')


"""Data Quality"""

# Reading the parquet file in curated layer
input_path = 'curated/silver_open_breweries'

df = read_parquet(input_path)


# Viewing the parquet file data
if df:
    df.show(truncate=False)
    logger.info(f'Viewing 20 first raw layer registers:\n {df.collect()[:20]}')
else:
    logger.error('No data to display.')


"""Applying Quality Tests"""

# 1. Checking non-empty dataset
try:
    assert df.count() > 0, 'The dataset is empty.'
except AssertionError as ae:
    logger.exception(f"Validation error: {str(ae)}")
    raise  # keeps the error for Airflow to mark as failure


# 2. Checking non-null data
try:
    assert df.filter(df.id.isNull() | df.name.isNull()).count() == 0, 'There are null values in the dataset.'
except AssertionError as ae:
    logger.exception(f"Validation error: {str(ae)}")
    raise


# 3. Checking ID uniqueness
try:
    assert df.groupBy('id').count().filter('count > 1').count() == 0, 'There are duplicate IDs in the dataset.'
except AssertionError as ae:
    logger.exception(f"Validation error: {str(ae)}")
    raise


# 4. Plausible Latitude and Longitude
try:
    assert df.filter((df.latitude < -90.0) | (df.latitude > 90.0) | (df.longitude < -180.0) | (df.longitude > 180.0)).count() == 0, 'There are invalid Latitude or Longitude values in the dataset.'
except AssertionError as ae:
    logger.exception(f"Validation error: {str(ae)}")
    raise


# 5. Validating the data types
expected_schema = {
    'id': 'string',
    'name': 'string',
    'brewery_type': 'string',
    'address_1': 'string',
    'address_2': 'string',
    'address_3': 'string',
    'city': 'string',
    'postal_code': 'string',
    'longitude': 'double',
    'latitude': 'double',
    'phone': 'string',
    'website_url': 'string',
    'is_active': 'boolean',
    'country': 'string',
    'state_province': 'string'}

try:
    for column, expected_type in expected_schema.items():
        actual_type = dict(df.dtypes)[column]  # df.dtypes returns list [(column, type)]
        assert actual_type == expected_type, f'Column "{column}" should be {expected_type}, but is {actual_type}.'
except AssertionError as ae:
    logger.exception(f"Validation error: {str(ae)}")
    raise