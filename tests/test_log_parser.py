import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, StringType
from src.log_parser.parser import LogParser  # Update the import path as needed
import os

# Set the PYSPARK_PYTHON environment variable
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'

@pytest.fixture(scope="module")
def spark_session():
    """Creates a Spark session for testing."""
    spark = SparkSession.builder.master("local[1]").appName("Test Session").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def fake_log_data(spark_session):
    """Generates a fake DataFrame similar to what would be read from the logs."""
    schema = StructType([
        StructField("timestamp", LongType(), nullable=False),
        StructField("host_from", StringType(), nullable=True),
        StructField("host_to", StringType(), nullable=True),
    ])
    data = [
        (1565647204351, "Aadvik", "Matina"),
        (1565647205599, "Keimy", "Dmetri"),
        (1565647212986, "Tyreonna", "Rehgan"),
        (1565647228897, "Heera", "Eron"),
        (1565647246869, "Jeremyah", "Morrigan"),
        (1565647247170, "Khiem", "Tailee"),
        (1565647256008, "Remiel", "Jadon"),
        (1565647260788, "Monet", "Jarreth"),
        (1565647264445, "Jil", "Cerena"),
        (1565647268712, "Naetochukwu", "Kallan"),
        (1565647289553, "Jahmani", "Markena")
    ]
    return spark_session.createDataFrame(data, schema=schema)

def test_get_connections_for_custom_period(spark_session, fake_log_data):
    """Tests fetching connections to a specific host within a custom time period."""
    log_parser = LogParser()
    time_init = 1565647220000  
    time_end = 1565647300000   
    hostname = "Eron"          

    # First filter data by time
    filtered_df = log_parser.filter_by_time(fake_log_data, time_init, time_end)
    # Then get connections to the specified hostname
    result_df = log_parser.get_connections(filtered_df, hostname)
    expected_data = [
        (1565647228897, "Heera")  # Expected result
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["timestamp", "host_from"])
    
    # Compare the dataframes
    assert result_df.collect() == expected_df.collect(), "DataFrames do not match the expected results"

if __name__ == "__main__":
    pytest.main()

