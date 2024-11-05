import os
import json
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_events_from_file(input_file_path):
    """Load events from a JSON file."""
    try:
        with open(input_file_path, "r") as f:
            events = json.load(f)
        return events
    except FileNotFoundError:
        logging.error(f"Input file not found: {input_file_path}")
        return None
    except json.JSONDecodeError:
        logging.error("Error decoding JSON from the input file.")
        return None

def process_events(events):
    """Process events to remove duplicates and extract date components."""
    events_df = pd.DataFrame(events)
    events_df = events_df.drop_duplicates(subset=["event_id"])
    events_df['timestamp'] = pd.to_datetime(events_df['timestamp'], errors='coerce')
    
    events_df['year'] = events_df['timestamp'].dt.year.astype(str)
    events_df['month'] = events_df['timestamp'].dt.month.astype(str).str.zfill(2)
    events_df['day'] = events_df['timestamp'].dt.day.astype(str).str.zfill(2)
    
    return events_df

def write_partitioned_data(events_df, base_output_path):
    """Write partitioned data to Parquet format."""
    os.makedirs(base_output_path, exist_ok=True)
    
    for (event_type, year, month, day), group_df in events_df.groupby(['event_type', 'year', 'month', 'day']):
        partition_dir = os.path.join(base_output_path, f"event_type={event_type}", f"year={year}", f"month={month}", f"day={day}")
        os.makedirs(partition_dir, exist_ok=True)
        parquet_file_path = os.path.join(partition_dir, "data.parquet")
        
        group_df.to_parquet(parquet_file_path, engine='pyarrow', index=False)
        logging.info(f"Written partitioned data to {parquet_file_path}")

def main():
    input_file_path = r"C:\Users\Anusuya\Downloads\Pismo\generateddata\events.json"
    base_output_path = r"C:\Users\Anusuya\Downloads\Pismo\organized_events"
    
    events = load_events_from_file(input_file_path)
    
    if events is not None:
        events_df = process_events(events)
        write_partitioned_data(events_df, base_output_path)

if __name__ == "__main__":
    main()
