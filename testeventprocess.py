import unittest
import pandas as pd
from src.event_processor import process_events

class TestEventProcessor(unittest.TestCase):
    def test_process_events(self):
        sample_events = [
            {
                "event_id": "1",
                "timestamp": "2023-11-05T12:00:00Z",
                "domain": "account",
                "event_type": "status-change",
                "data": {
                    "id": 123456,
                    "old_status": "ACTIVE",
                    "new_status": "SUSPENDED",
                    "reason": "Fraud investigation"
                }
            },
            {
                "event_id": "2",
                "timestamp": "2023-11-05T12:00:00Z",
                "domain": "transaction",
                "event_type": "payment",
                "data": {
                    "id": 654321,
                    "old_status": "COMPLETED",
                    "new_status": "REFUNDED",
                    "reason": "Customer requested refund"
                }
            }
        ]
        
        events_df = process_events(sample_events)
        self.assertEqual(events_df.shape[0], 2)  # Two events processed
        self.assertIn('year', events_df.columns)
        self.assertIn('month', events_df.columns)
        self.assertIn('day', events_df.columns)

if __name__ == "__main__":
    unittest.main()
