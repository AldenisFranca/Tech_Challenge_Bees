"""extract.py

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
import requests
from utils import get_logger

"""Extract (Bronze/Raw Layer)

Persist the raw data from the API in its native format or
any format you find suitable.
"""

# Function to make requests in the API
def get_all_data_from_api(base_url: str, page_param: str, per_page_param: str, page_start: int = 1, items_per_page: int = 50):

    """
    Function that makes paginated requests to an API and returns a list with all the pages data.

    Args:
        base_url: API base URL.
        page_param: Page parameter name in the URL.
        per_page_param: Items per page parameter name.
        page_start: First page number (default: 1).
        items_per_page: Quantity of items per page (default: 50).

    Returns:
        List with all the pages data.
    """

    all_data = []
    page = page_start

    while True:
        url = f'{base_url}?{page_param}={page}&{per_page_param}={items_per_page}'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            if not data:
                break  # exit the loop if there is no more data

            all_data.extend(data)
            page += 1
        elif response.status_code == 429:
            all_data = []
            page = page_start
        else:
            print(f'Request error (page {page}): {response.status_code}')
            break  # exit the loop on error

    return all_data

# Starting the logging function
logger = get_logger('Extract')

# Entering the API URL
base_url = 'https://api.openbrewerydb.org/v1/breweries'

# Function call
complet_data = get_all_data_from_api(base_url, page_param = 'page', per_page_param = 'per_page')

# Number of rows in the complete database
logger.info(f'Number of rows in the complete database: {len(complet_data)}')

# Creating the raw folder
try:
    os.mkdir('raw')
except:
    pass

# Serializing the python list into a json string
json_data = json.dumps(complet_data, indent = 4)

# Saving API data in the raw/bronze layer
output_path = 'raw/bronze_open_breweries.json'
with open(output_path, 'w') as file:
    file.write(json_data)

# Validating if the file exists
if os.path.exists(output_path):
    logger.info(f'Data saved in {output_path}')
else:
    logger.error('Error: Directory not found.')