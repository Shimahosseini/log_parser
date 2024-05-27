from pyspark.sql.types import StructType, StructField, LongType, StringType

# Define the schema for the log data
log_schema = StructType([
    StructField("timestamp", LongType(), nullable=False),
    StructField("host_from", StringType(), nullable=True),
    StructField("host_to", StringType(), nullable=True),
])
