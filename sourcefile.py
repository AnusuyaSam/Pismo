import os
import json
from datetime import datetime
from faker import Faker

 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Faker instance
fake = Faker()

def create_output_directory(output_file_path):
    output_dir = os.path.dirname(output_file_path)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Output directory created or already exists: {output_dir}")

def generate_events(num_records=10):
    """Generate synthetic events with random data."""
    event_types = ["account", "transaction"]
    events = []

    for _ in range(num_records):
        domain = random.choice(event_types)  
        timestamp = fake.date_time_between(start_date='-5y', end_date='now').isoformat()

        event = {
            "event_id": str(fake.uuid4()),
            "timestamp": timestamp,
            "domain": domain,
            "event_type": "status-change" if domain == "account" else "payment",
            "data": {
                "id": fake.random_number(digits=6),
                "old_status": random.choice(["ACTIVE", "SUSPENDED"]),
                "new_status": random.choice(["ACTIVE", "SUSPENDED"]),
                "reason": fake.sentence()
            }
        }
        events.append(event)

    return events

def save_events_to_file(events, output_file_path):
    """Save generated events to a JSON file."""
    try:
        with open(output_file_path, "w") as json_file:
            json.dump(events, json_file, indent=4)
        logging.info(f"All events written to {output_file_path}")
    except Exception as e:
        logging.error(f"Failed to write events to file: {e}")

def main():
    output_file_path = r"C:\Users\Anusuya\Downloads\Pismo\generateddata\events.json"
    create_output_directory(output_file_path)
    events = generate_events(num_records=10)
    save_events_to_file(events, output_file_path)

if __name__ == "__main__":
    main()
