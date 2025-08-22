"""load.py

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
logger = get_logger('Load')
spark = create_spark('Load to Gold Layer')

"""Load (Gold/Analytical Layer)

Create an aggregated view with the quantity of breweries per type and location.
"""

# Function that aggregates breweries by type and location
def aggregate_breweries(input_path: str, output_path: str):

    """
    Function that aggregates breweries by type and location and writes the results to a parquet file.

    Args:
        input_path (str): the path to the input parquet file.
        output_path (str): the path to the output parquet file.

    Returns:
        agg_df: an aggregated dataframe by type and location of the breweries as a PySpark dataframe.
    """

    # Aggregation: number of breweries by type and location
    agg_df = (
        df.groupBy('country', 'state_province', 'brewery_type')
          .agg(count('*').alias('brewery_count'))
          .orderBy(col('brewery_count').desc())
    )

    # Saving the aggregated data in gold/analytical layer
    agg_df.write.mode('overwrite').parquet(output_path)

    return agg_df


# Reading the parquet file from curated layer
input_path = 'curated/silver_open_breweries'
output_path = 'analytical/gold_open_breweries'

df = read_parquet(input_path)


# Viewing the parquet file data
if df:
    df.show(truncate=False)
    logger.info(f'Viewing 20 first raw layer registers:\n {df.collect()[:20]}')
else:
    logger.error('No data to display.')


# Function call
df_analytical = aggregate_breweries(input_path, output_path)


# Validation
if df_analytical.count() > 0:
    logger.info('Aggregated data saved successfully.')
else:
    logger.error('No data to display.')