from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from log_parser.schema import log_schema 

class LogParser:
    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("Network Log Parser").getOrCreate()

    def read_logs(self, file_path: str):
        # Read logs from a text file with space-separated values using the predefined schema
        return self.spark.read.csv(file_path, schema=log_schema, sep=" ", header=False)

    def filter_by_time(self, df, time_init: int, time_end: int):
        # Filter DataFrame rows between start and end timestamps
        return df.filter((col('timestamp') >= time_init) & (col('timestamp') <= time_end))

    def get_connections(self, df, hostname: str):
        # Retrieve all unique hosts connected to and from the given hostname within the DataFrame
        connections_to = df.filter(col('host_from') == hostname).select('host_to').distinct().rdd.map(lambda r: r[0]).collect()
        connections_from = df.filter(col('host_to') == hostname).select('host_from').distinct().rdd.map(lambda r: r[0]).collect()
        return {"to": connections_to, "from": connections_from}

    def run(self, file_path: str, time_init: int, time_end: int, hostname: str):
        # Process the log file to find connections to/from the given hostname within the time range
        df = self.read_logs(file_path)
        df_filtered = self.filter_by_time(df, time_init, time_end)
        return self.get_connections(df_filtered, hostname)
