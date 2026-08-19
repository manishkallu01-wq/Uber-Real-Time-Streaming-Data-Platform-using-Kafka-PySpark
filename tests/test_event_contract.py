import unittest
from src.event_contract import validate_event

class EventContractTests(unittest.TestCase):
    def valid(self):
        return {"schema_version":1,"event_id":"e1","trip_id":"t1","event_time":"2026-01-01T00:00:00Z","city":"Austin","fare_usd":12.5,"distance_km":3.2}
    def test_valid_event(self): self.assertEqual(validate_event(self.valid()),[])
    def test_missing_field(self):
        event=self.valid(); del event["city"]
        self.assertIn("missing city",validate_event(event))
    def test_rejects_negative_measure(self):
        event=self.valid(); event["fare_usd"]=-1
        self.assertIn("fare_usd must be non-negative",validate_event(event))
    def test_rejects_unknown_schema(self):
        event=self.valid(); event["schema_version"]=2
        self.assertIn("unsupported schema_version",validate_event(event))

if __name__=="__main__": unittest.main()
