import os
import json
import pandas as pd
from datetime import datetime

# Define paths
input_file_path = r"C:\Users\Anusuya\Downloads\Pismo\generateddata\events.json"  # Path to the events JSON file
base_output_path = r"C:\Users\Anusuya\Downloads\Pismo\organized_events"  # Path for organized events

# Check if the input file exists before proceeding
if not os.path.exists(input_file_path):
    print(f"Input file not found: {input_file_path}")
else:
    try:
        # Load JSON data into a list of dictionaries
        with open(input_file_path, "r") as f:
            events = json.load(f)

        # Convert list of events to a DataFrame
        events_df = pd.DataFrame(events)

        # Drop duplicates based on 'event_id'
        events_df = events_df.drop_duplicates(subset=["event_id"])

        # Ensure timestamp column is in datetime format
        events_df['timestamp'] = pd.to_datetime(events_df['timestamp'], errors='coerce')

        # Extract year, month, and day as separate columns for partitioning
        events_df['year'] = events_df['timestamp'].dt.year.astype(str)
        events_df['month'] = events_df['timestamp'].dt.month.astype(str).str.zfill(2)
        events_df['day'] = events_df['timestamp'].dt.day.astype(str).str.zfill(2)

        # Check and create the base output directory if it doesn't exist
        os.makedirs(base_output_path, exist_ok=True)

        # Write partitioned data to Parquet format
        for (event_type, year, month, day), group_df in events_df.groupby(
            ['event_type', 'year', 'month', 'day']
        ):
            # Define the partitioned directory structure
            partition_dir = os.path.join(
                base_output_path,
                f"event_type={event_type}",
                f"year={year}",
                f"month={month}",
                f"day={day}"
            )
            os.makedirs(partition_dir, exist_ok=True)

            # Define the Parquet file path
            parquet_file_path = os.path.join(partition_dir, "data.parquet")

            # Write DataFrame to Parquet format
            group_df.to_parquet(parquet_file_path, engine='pyarrow', index=False)

        print(f"Events successfully written to {base_output_path} in Parquet format.")

    except json.JSONDecodeError:
        print("Error decoding JSON from the input file.")
    except Exception as e:
        print(f"An error occurred while processing the input file: {e}")
