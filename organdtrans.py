from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, year, month, dayofmonth
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OrganizeEvents") \
    .getOrCreate()

# Define paths
input_file_path = r"C:\Users\Anusuya\Downloads\Pismo\generateddata\events.json"  # Path to the events JSON file
base_output_path = r"C:\Users\Anusuya\Downloads\Pismo\organized_events"  # Path for organized events

# Check if the input file exists before proceeding
if not os.path.exists(input_file_path):
    print(f"Input file not found: {input_file_path}")
else:
    try:
        # Load JSON data into a Spark DataFrame
        events_df = spark.read.json(input_file_path)

        # Drop duplicates based on 'event_id'
        events_df = events_df.dropDuplicates(["event_id"])

        # Ensure timestamp column is in datetime format
        events_df = events_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

        # Create year, month, day columns for date-based partitioning
        events_df = events_df.withColumn("year", year(col("timestamp")).cast("string")) \
                             .withColumn("month", month(col("timestamp")).cast("string").rjust(2, '0')) \
                             .withColumn("day", dayofmonth(col("timestamp")).cast("string").rjust(2, '0'))

        # Create a unique "event_type_combined" by combining "domain" and "event_type"
        events_df = events_df.withColumn("event_type_combined", concat_ws("_", col("domain"), col("event_type")))

        # Write data to Parquet format partitioned by event_type_combined, year, month, and day
        events_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("event_type_combined", "year", "month", "day") \
            .save(base_output_path)

        print(f"Events successfully written to {base_output_path} in Parquet format, partitioned by event_type_combined and date.")

    except Exception as e:
        print(f"An error occurred while processing the input file: {e}")

# Stop Spark session
spark.stop()
