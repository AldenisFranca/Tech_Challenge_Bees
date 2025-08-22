"""utils.py
"""

# Importing the Libraries
import os
import logging
from pyspark.sql import SparkSession


# -----------------------------
# Logging Functions
# -----------------------------
def get_logger(name: str, log_file: str = 'logs/pipeline.log'):
    """
    Creates and returns a logger configured for console and file.

    Args:
        name (str): Logger name (e.g.: 'Transform', 'Aggregate').
        log_file (str): Path of log file, default is 'logs/pipeline.log'

    Returns:
        logging.Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Evita adicionar handlers duplicados
    if not logger.handlers:
        # Criar pasta de logs caso não exista
        os.makedirs(os.path.dirname(log_file), exist_ok = True)

        # Formatter padrão
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler para arquivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# -----------------------------
# Spark Functions
# -----------------------------
def create_spark(app_name: str) -> SparkSession:

    """
    Function that create a Spark Session

    Args:
        app_name (str): Name of the Spark Application

    Returns:
        SparkSession: Spark Session

    """
    spark = (
            SparkSession
            .builder
            .appName(app_name)
            .getOrCreate())

    return spark


# -----------------------------
# Read Functions
# -----------------------------
def read_parquet(file_path):

    """
    Function that reads a parquet file and returns its contents as a PySpark dataframe.

    Args:
        file_path (str): The path to the parquet file.

    Returns:
        df: The contents of the parquet file as a PySpark dataframe.
        None: If an error occurred while opening or reading the file.
    """

    try:
        df = spark.read.parquet(file_path)
        return df
    except FileNotFoundError:
        print(f"Error: File not found in {file_path}")
        return None
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return None