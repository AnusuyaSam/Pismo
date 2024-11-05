from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OrganizeEvents") \
    .getOrCreate()

# Define paths
input_file_path = r"C:\Users\Anusuya\Downloads\Pismo\generateddata\events.json"  # Path to the events JSON file
output_file_path = r"C:\Users\Anusuya\Downloads\Pismo\organized_events\data.parquet"  # Path for the single Parquet file

# Check if the input file exists before proceeding
if not os.path.exists(input_file_path):
    print(f"Input file not found: {input_file_path}")
else:
    try:
        # Load JSON data into a Spark DataFrame
        events_df = spark.read.json(input_file_path)

        # Drop duplicates based on 'event_id'
        events_df = events_df.dropDuplicates(["event_id"])

        # Ensure timestamp column is in datetime format and create year, month, day columns
        events_df = events_df.withColumn("timestamp", col("timestamp").cast("timestamp")) \
                             .withColumn("year", year(col("timestamp")).cast("string")) \
                             .withColumn("month", month(col("timestamp")).cast("string").rjust(2, '0')) \
                             .withColumn("day", dayofmonth(col("timestamp")).cast("string").rjust(2, '0'))

        # Write data explicitly in Parquet format
        events_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(output_file_path)

        print(f"Events successfully written to {output_file_path} in Parquet format.")

    except Exception as e:
        print(f"An error occurred while processing the input file: {e}")

# Stop Spark session
spark.stop()
