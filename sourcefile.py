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
num_records = 10  # Number of synthetic events to generate
event_types = ["account", "transaction"]

# Generate synthetic events
events = []  # List to store all generated events

for _ in range(num_records):
    domain = random.choice(event_types)  # Randomly select a domain
    timestamp = fake.date_time_between(start_date='-5y', end_date='now')  # Generate a random timestamp

    # Create the event dictionary
    event = {
        "event_id": str(fake.uuid4()),  # Generate a unique event ID
        "timestamp": timestamp.isoformat(),  # Format timestamp in ISO format
        "domain": domain,
        "event_type": "status-change" if domain == "account" else "payment",
        "data": {
            "id": fake.random_number(digits=6),  # Generate a random number for the ID
            "old_status": random.choice(["ACTIVE", "SUSPENDED"]),  # Random old status
            "new_status": random.choice(["ACTIVE", "SUSPENDED"]),  # Random new status
            "reason": fake.sentence()  # Generate a random reason
        }
    }

    # Append the generated event to the events list
    events.append(event)

# Write all events to a single JSON file
try:
    with open(output_file_path, "w") as json_file:
        json.dump(events, json_file, indent=4)  # Write the list of events to JSON
    print(f"All events written to {output_file_path}")
except Exception as e:
    print(f"Failed to write events to file: {e}")
