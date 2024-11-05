import os
import json
from datetime import datetime
from faker import Faker
import random

# Initialize Faker instance
fake = Faker()

# Define the output file path for the generated events
output_file_path = r"C:\Users\Anusuya\Downloads\Pismo\generateddata\events.json"

# Ensure the output directory exists
output_dir = os.path.dirname(output_file_path)
try:
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory created or already exists: {output_dir}")
except Exception as e:
    print(f"Failed to create output directory: {e}")

# Event generation settings
num_records = 10  # Base number of unique synthetic events to generate
event_types = ["account", "transaction"]
all_events = []  # List to store all generated events, including duplicates and errors

# Generate events with intentional errors
for _ in range(num_records):
    domain = random.choice(event_types)
    timestamp = fake.date_time_between(start_date='-5y', end_date='now')
    event_type = "status-change" if domain == "account" else "payment"

    # Randomly introduce errors in event data
    if random.choice([True, False]):
        # 50% chance to introduce an invalid timestamp format
        timestamp = "INVALID_TIMESTAMP" if random.choice([True, False]) else timestamp.isoformat()

    # Create event with inconsistent data ID or random event type
    if random.choice([True, False]):
        # 50% chance to assign a data ID type inconsistent with the domain
        data_id = fake.random_number(digits=6) if domain == "account" else fake.uuid4()
    else:
        data_id = fake.random_number(digits=6)

    # Introduce a random unrelated event type to some events
    event_type = "unexpected_type" if random.choice([True, False]) else event_type

    # Create the event dictionary
    event = {
        "event_id": str(fake.uuid4() if random.choice([True, False]) else "DUPLICATE_ID"),
        "timestamp": timestamp,
        "domain": domain,
        "event_type": event_type,
        "data": {
            "id": data_id,
            "old_status": random.choice(["ACTIVE", "SUSPENDED"]),
            "new_status": random.choice(["ACTIVE", "SUSPENDED"]),
            "reason": fake.sentence()
        }
    }

    # Append the generated event to the list of events
    all_events.append(event)

# Write all events to a single JSON file
try:
    with open(output_file_path, "w") as json_file:
        json.dump(all_events, json_file, indent=4)
    print(f"All events, including intentional errors, written to {output_file_path}")
except Exception as e:
    print(f"Failed to write events to file: {e}")
