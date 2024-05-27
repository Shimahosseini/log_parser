from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import col
from typing import List

class LogParser:
    """
    A class to parse and analyze network log data using Apache Spark.

    Attributes:
        spark (SparkSession): An instance of SparkSession used to perform data processing.

    Methods:
        read_logs(file_path: str) -> DataFrame:
            Reads log data from a specified file using a predefined schema.

        filter_by_time(df: DataFrame, time_init: int, time_end: int) -> DataFrame:
            Filters a DataFrame to include only the entries between specified start and end timestamps.

        get_connections(df: DataFrame, hostname: str) -> DataFrame:
            Retrieves all unique connection entries to a specified hostname.

        get_connections_to(df: DataFrame, hostname: str, current_time: int) -> DataFrame:
            Retrieves all hosts that connected to a specified hostname within the last hour from a given time.

        get_connections_from(df: DataFrame, hostname: str, current_time: int) -> DataFrame:
            Retrieves all hosts that a specified hostname has connected to within the last hour from a given time.

        most_active_host(df: DataFrame, current_time: int) -> DataFrame:
            Identifies the host with the most connections in the last hour from a given time.

        run(file_path: str, current_time: int, hostname: str):
            Executes log analysis tasks including connections to and from a host, and identifying the most active host.
    """
    def __init__(self):
        # Initialize Spark session
        self.spark: SparkSession = SparkSession.builder.appName("Network Log Parser").getOrCreate()

    def read_logs(self, file_path: str) -> DataFrame:
        """
        Reads log data from a specified file, assuming a predefined schema and space-separated values.
        
        Args:
            file_path (str): The path to the log file.
        
        Returns:
            DataFrame: A DataFrame containing the sorted log data.
        """
        log_schema = StructType([
            StructField("timestamp", LongType(), nullable=False),
            StructField("host_from", StringType(), nullable=True),
            StructField("host_to", StringType(), nullable=True),
        ])
        df = self.spark.read.csv(file_path, schema=log_schema, sep=" ", header=False)
        df_sorted = df.sort(col("timestamp"))
        return df_sorted

    def filter_by_time(self, df: DataFrame, time_init: int, time_end: int) -> DataFrame:
        """
        Filters the DataFrame to only include entries between the specified start and end timestamps.

        Args:
            df (DataFrame): The DataFrame to filter.
            time_init (int): The start timestamp in milliseconds.
            time_end (int): The end timestamp in milliseconds.

        Returns:
            DataFrame: The filtered DataFrame.
        """
        return df.filter((col('timestamp') >= time_init) & (col('timestamp') <= time_end))

    def get_connections(self, df: DataFrame, hostname: str) -> DataFrame:
        """
        Retrieves all unique connections to the given hostname within the DataFrame.

        Args:
            df (DataFrame): The DataFrame to query.
            hostname (str): The hostname to which connections are sought.

        Returns:
            DataFrame: A DataFrame of unique connections to the specified hostname.
        """
        return df.filter(col('host_to') == hostname).select('timestamp', 'host_from').distinct()

    def get_connections_to(self, df: DataFrame, hostname: str, current_time: int) -> DataFrame:
        """
        Retrieves all distinct hostnames that connected to the specified host within the last hour from the given current time.

        Args:
            df (DataFrame): The DataFrame containing log data.
            hostname (str): The hostname to which connections are sought.
            current_time (int): The current time in milliseconds used to define the last hour window.

        Returns:
            DataFrame: A DataFrame containing distinct hostnames that connected to the specified host.
        """
        one_hour_ago = current_time - 3600000  # Subtract one hour in milliseconds
        return df.filter((col('host_to') == hostname) & (col('timestamp') > one_hour_ago)).select('host_from').distinct()

    def get_connections_from(self, df: DataFrame, hostname: str, current_time: int) -> DataFrame:
        """
        Retrieves all distinct hostnames that the specified host connected to within the last hour from the given current time.

        Args:
            df (DataFrame): The DataFrame containing log data.
            hostname (str): The hostname from which connections are sought.
            current_time (int): The current time in milliseconds used to define the last hour window.

        Returns:
            DataFrame: A DataFrame containing distinct hostnames that the specified host connected to.
        """
        one_hour_ago = current_time - 3600000  # Subtract one hour in milliseconds
        return df.filter((col('host_from') == hostname) & (col('timestamp') > one_hour_ago)).select('host_to').distinct()

    def most_active_host(self, df: DataFrame, current_time: int) -> DataFrame:
        """
        Identifies the host that generated the most connections within the last hour from the given current time.

        Args:
            df (DataFrame): The DataFrame to analyze.
            current_time (int): The current time in milliseconds to define the last hour window.

        Returns:
            DataFrame: A DataFrame containing the most active host.
        """
        one_hour_ago = current_time - 3600000  # Subtract one hour in milliseconds
        return df.filter(col('timestamp') > one_hour_ago).groupBy('host_from').count().orderBy(col('count').desc()).limit(1)

    def run(self, file_path: str, current_time: int, hostname: str):
        """
        Runs the full analysis tasks for the log data based on given parameters.
        
        Args:
            file_path (str): Path to the log file.
            current_time (int): Current time in milliseconds for time-based filtering.
            hostname (str): Hostname to analyze connections to and from.
        """
        df_sorted = self.read_logs(file_path)
        connections_to_last_hour = self.get_connections_to(df_sorted, hostname, current_time)
        connections_from_last_hour = self.get_connections_from(df_sorted, hostname, current_time)
        top_host_last_hour = self.most_active_host(df_sorted, current_time)

        # Display results
        connections_to_last_hour.show()
        connections_from_last_hour.show()
        top_host_last_hour.show()

