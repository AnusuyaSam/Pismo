import unittest
from src.data_generator import generate_events

class TestDataGenerator(unittest.TestCase):
    def test_generate_events(self):
        events = generate_events(num_records=5)
        self.assertEqual(len(events), 5)
        self.assertTrue(all('event_id' in event for event in events))
        self.assertTrue(all('timestamp' in event for event in events))
        self.assertTrue(all('domain' in event for event in events))

if __name__ == "__main__":
    unittest.main()
