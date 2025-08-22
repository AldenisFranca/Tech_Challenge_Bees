"""transform.py

Tech Challenge - Data Engineer - AB-InBev | Bees
Candidato: Aldenis França

Objective:
    The goal of this test is to assess your skills in consuming data from an API, transforming and
    persisting it into a data lake following the medallion architecture with three layers: raw data,
    curated data partitioned by location, and an analytical aggregated layer.
"""

# Importing the Libraries
import os
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import get_logger, create_spark

"""Starting a PySpark Session"""

# Creating the Spark Session
logger = get_logger('Transform')
spark = create_spark('Transform Silver Layer')

"""Transform (Silver/Curated Layer)

Transform the data to a columnar storage format such as
parquet or delta, and partition it by brewery location. Please explain any
other transformations you perform.
"""

# Function to read the json file with the bronze/raw layer data
def read_json(file_path):

    """
      Function that reads a JSON file and returns its contents as a Python object.

      Args:
          file_path(str): Path to the JSON file.

      Returns:
          data: Contents of the JSON file as a Python object (dictionary, list, etc.).
          None: If an error occurs while opening or reading the file.
    """

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: File Not Found in {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: JSON format invalid in {file_path}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


# Function that remove columns with identical data
def remove_identical_columns(pyspark_dataframe, col1: str, col2: str):

    """
      Function that removes a column with identical data another column from a PySpark DataFrame.

      Args:
          pyspark_dataframe: a PySpark DataFrame containing the data to be checked.
          col1: the name of the first column to be checked.
          col2: the name of the second column to be checked and that can be deleted.

      Returns:
          pyspark_dataframe: a PySpark DataFrame with the identical column(s) removed.
    """

    if pyspark_dataframe.where(col(col1) != col(col2)).count() == 0:
        pyspark_dataframe = pyspark_dataframe.drop(col2)
    return pyspark_dataframe


# Function that check plausible Latitude/Longitude values
def check_plausible_longitude_latitude_values(pyspark_dataframe, col_longitude: str, col_latitude: str):

    """
      Function that checks the plausible values for Latitude and Longitude.

      Args:
          pyspark_dataframe: a PySpark DataFrame containing the data to be checked.
          col_longitude: the name of the column containing the Longitude values.
          col_latitude: the name of the column containing the Latitude values

      Returns:
          qtd_implausible_long: the quantity of invalid Longitude values.
          qtd_implausible_lat: the quantity of invalid Latitude values.
          qtd_lat_90_180: the quantity of Latitude values between +90 and +180
    """

    # Checking plausible Longitude (values between -180/+180)
    qtd_implausible_long = pyspark_dataframe.filter((col(col_longitude) < -180.0) | (col(col_longitude) > 180.0)).count()
    logger.info(f'Quantity of implausible Longitude values: {qtd_implausible_long}\n')

    # Checking plausible Latitude (values between -90/+90)
    qtd_implausible_lat = pyspark_dataframe.filter((col(col_latitude) < -90.0) | (col(col_latitude) > 90.0)).count()
    logger.info(f'Quantity of implausible Latitude values: {qtd_implausible_lat}\n')

    # Checking the quantity of Latitude with values between +90 and +180)
    qtd_lat_90_180 = pyspark_dataframe.filter((col(col_latitude) > 90.0) & (col(col_latitude) <= 180.0)).count()
    logger.info(f'Quantity of Latitude with values between +90 and +180: {qtd_lat_90_180}\n')

    return qtd_implausible_long, qtd_implausible_lat, qtd_lat_90_180


# Function that fix implausible latitude values between +90 and +180 and longitude values between -90 and +90
def fix_implausible_latitude_values(pyspark_dataframe, col_longitude: str, col_latitude: str):

    """
      Function that fixes the implausible latitude values between +90 and +180, when longitude values between -90 and +90.

      Args:
          pyspark_dataframe: a PySpark DataFrame containing the data to be checked.
          col_longitude: the name of the column containing the Longitude values.
          col_latitude: the name of the column containing the Latitude values

      Returns:
          pyspark_dataframe: a PySpark DataFrame with the fixed values.
    """

    if qtd_checking[1] > 0:

        if pyspark_dataframe.filter(((col(col_latitude) > 90.0) & (col(col_latitude) <= 180.0)) & ((col(col_longitude) >= -90.0) & (col(col_longitude) <= 90.0))):

            pyspark_dataframe = pyspark_dataframe.withColumn('aux_longitude', col(col_longitude)).withColumn('aux_latitude', col(col_latitude))
            pyspark_dataframe = pyspark_dataframe.withColumn(col_longitude, when(((col('aux_latitude') > 90.0) & (col('aux_latitude') <= 180.0)) & ((col('aux_longitude') >= -90.0) & (col('aux_longitude') <= 90.0)), col('aux_latitude')) \
                                                 .otherwise(col(col_longitude)))
            pyspark_dataframe = pyspark_dataframe.withColumn(col_latitude, when(((col('aux_latitude') > 90.0) & (col('aux_latitude') <= 180.0)) & ((col('aux_longitude') >= -90.0) & (col('aux_longitude') <= 90.0)), col('aux_longitude')) \
                                                 .otherwise(col(col_latitude)))
            pyspark_dataframe = pyspark_dataframe.drop('aux_longitude', 'aux_latitude')

    return pyspark_dataframe



"""Reading Data and Building Dataframe"""

# Reading the json file in raw layer
input_path = 'raw/bronze_open_breweries.json'
output_path = 'curated/silver_open_breweries'

read_data = read_json(input_path)

# Viewing json file data
if read_data:
    logger.info(f'Read {len(read_data)} raw layer registers')

# Creating a DataFrame from the json data and informing a specific schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True)
])

df = spark.createDataFrame(read_data, schema)
logger.info(f'Viewing 20 first raw layer registers:\n {df.collect()[:20]}')


"""Applying Transformings"""

# Removing duplicate rows based on ID
df = df.dropDuplicates(['id'])


# Function call
df = remove_identical_columns(df, 'state_province', 'state')
df = remove_identical_columns(df, 'address_1', 'street')
df = remove_identical_columns(df, 'address_1', 'address_2')
df = remove_identical_columns(df, 'address_1', 'address_3')


# Normalize the data by removing spaces and converting to Title Case
for column in ['name', 'address_1', 'address_2', 'address_3', 'city', 'state_province', 'country', 'state', 'street']:

    if column in df.columns:
        df = df.withColumn(column, trim(initcap(col(column))))


# Function call
qtd_checking = check_plausible_longitude_latitude_values(df, 'longitude', 'latitude')


# Function call
df = fix_implausible_latitude_values(df, 'longitude', 'latitude')


# Fixing a specific case of invalid latitude values above +180
if df.filter(((col('latitude') < -90.0) | (col('latitude') > 90.0))):

    df = df.withColumn('latitude', when(((col('latitude') < -90.0) | (col('latitude') > 90.0)), regexp_replace(col("latitude"), r"^.{5}", "")).otherwise(col('latitude')))


# Showing just the corrected rows
df_fixed = df.filter((col('state_province') == 'Singapore') | (col('id') == 'bcc3a506-1067-482c-90a4-0979423987d9'))
df_fixed.show(40, truncate=False)
logger.info(f'Viewing the fixed registers:\n {df_fixed.collect()[:40]}')


# Converting the data type of the latitude and longitude columns from string to double
df = df.withColumn('longitude', col('longitude').cast('double'))
df = df.withColumn('latitude', col('latitude').cast('double'))


# Creating a column showing whether the brewery is in operation
df = df.withColumn('is_active', ((df.brewery_type != 'closed') & (df.brewery_type != 'planning')).cast('boolean'))
logger.info(f'Viewing first 20 rows after all transformations:\n {df.collect()[:20]}')


# Saving the transformed data in silver/curated layer
df.write.mode('overwrite') \
        .partitionBy('country', 'state_province') \
        .parquet(output_path)


# Validating if the file exists
if os.path.exists(output_path):
    logger.info(f'Data saved in {output_path}')
else:
    logger.error('Error: Directory not found.')