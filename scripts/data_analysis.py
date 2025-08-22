"""data_analysis.py

Tech Challenge - Data Engineer - AB-InBev | Bees
Candidato: Aldenis França

Objective:
    The goal of this test is to assess your skills in consuming data from an API, transforming and
    persisting it into a data lake following the medallion architecture with three layers: raw data,
    curated data partitioned by location, and an analytical aggregated layer.
"""

# Importing the Libraries
import folium
from pyspark.sql.types import *
from pyspark.sql.functions import *
from utils import get_logger, create_spark, read_parquet


"""Starting a PySpark Session"""

# Creating the Spark Session
logger = get_logger('Analysis')
spark = create_spark('Data Analysis')


"""Data Analysis"""

# Reading the parquet file from curated layer
curated_path = 'curated/silver_open_breweries'
analytical_path = 'analytical/gold_open_breweries'

df = read_parquet(curated_path)


# Viewing the parquet file data
if df:
    df.show(truncate=False)
    logger.info(f'Viewing 20 first raw layer registers:\n {df.collect()[:20]}')
else:
    logger.error('No data to display.')


# Checking the quantity of rows and columns
logger.info(f'Quantity of dataset rows: {df.count()}')
logger.info(f'Quantity of dataset columns: {len(df.columns)}\n\n')


# Checking column data types
logger.info(f'Checking column data types:\n {df.dtypes}')
df.printSchema()


# Statistical Analysis on the Dataset
logger.info(f'\nStatistical Analysis on the Dataset:\n {df.describe().collect()}')
df.describe().show()


# Quantity of null data per column
logger.info(f'Quantity of null data per column: \n {df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()}')
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# Quantity of distincts countries, states, cities and brewery types
distinct_countries = df.select('country').distinct().count()
distinct_states = df.select('state_province').distinct().count()
distinct_cities = df.select('city').distinct().count()
distinct_brewery_types = df.select('brewery_type').distinct().count()

logger.info(f'Distinct quantity of countries: {distinct_countries}\n')
logger.info(f'Distinct quantity of states: {distinct_states}\n')
logger.info(f'Distinct quantity of cities: {distinct_cities}\n')
logger.info(f'Distinct quantity of brewery types: {distinct_brewery_types}\n')


# Create a world map centered at a general location
world_map = folium.Map(location = [39.8283, -98.5795], zoom_start = 3)

# Add markers for each brewery with valid latitude and longitude
for row in df.collect():
    lat = row['latitude']
    lon = row['longitude']
    name = row['name']
    city = row['city']
    state_province = row['state_province']
    country = row['country']
    brewery_type = row['brewery_type']

    if lat is not None and lon is not None:
        popup_text = f"<b>{name}</b><br>{city}, {state_province}, {country}<br>Type: {brewery_type}"
        folium.Marker([lat, lon], popup=popup_text).add_to(world_map)

# Display the map
world_map


"""Insights based on Aggregate Vision"""


# Reading data from gold layer
df_analytical = read_parquet(analytical_path)

# Converting to a Temporary View
df_analytical.createOrReplaceTempView('breweries_agg')


# 1. Qual o estado com o maior número de cervejarias e qual o com o menor número de cervejarias?

print('Qual o estado com o maior número de cervejarias e qual o com o menor número de cervejarias?')
spark.sql("""
    SELECT state_province, brewery_count
    FROM breweries_agg
    WHERE brewery_count = (SELECT MAX(brewery_count) FROM breweries_agg)
    OR brewery_count = (SELECT MIN(brewery_count) FROM breweries_agg)
""").show()

# 2. Qual estado possui o maior número de cervejarias que NÃO estão em funcionamento?

print('Qual estado possui o maior número de cervejarias que NÃO estão em funcionamento?')
spark.sql("""
    SELECT *
    FROM breweries_agg
    WHERE brewery_type IN ('closed', 'planning')
    AND brewery_count = (SELECT MAX(brewery_count) FROM breweries_agg WHERE brewery_type IN ('closed', 'planning'))
""").show()

# 3. Dentre todos os países, qual o tipo de cervejaria que mais existe e qual a quantidade?

print('Dentre todos os países, qual o tipo de cervejaria que mais existe e qual a quantidade?')
spark.sql("""
    SELECT brewery_type, SUM(brewery_count) AS sum_brewery_count
    FROM breweries_agg
    GROUP BY brewery_type
    ORDER BY sum_brewery_count DESC
    LIMIT 1
""").show()

# 4. E qual o tipo de cervejaria menos comum?

print('E qual o tipo de cervejaria menos comum?')
spark.sql("""
    SELECT brewery_type, SUM(brewery_count) AS sum_brewery_count
    FROM breweries_agg
    GROUP BY brewery_type
    ORDER BY sum_brewery_count
    LIMIT 1
""").show()