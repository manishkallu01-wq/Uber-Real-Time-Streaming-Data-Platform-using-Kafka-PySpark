"""Emit deterministic sample trips to Kafka."""
import json, os, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from event_contract import validate_event

producer=KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"),value_serializer=lambda v:json.dumps(v).encode(),key_serializer=lambda v:v.encode())
topic=os.getenv("TRIP_TOPIC","trip-events")
for sequence in range(int(os.getenv("EVENT_COUNT","100"))):
    event={"schema_version":1,"event_id":str(uuid.uuid4()),"trip_id":f"trip-{sequence:06d}","event_time":datetime.now(timezone.utc).isoformat(),"city":["Austin","Chicago","San Francisco"][sequence%3],"fare_usd":round(8+sequence%40*1.25,2),"distance_km":round(1+sequence%20*.7,2)}
    errors=validate_event(event)
    if errors: raise ValueError(errors)
    producer.send(topic,key=event["trip_id"],value=event)
    time.sleep(float(os.getenv("EVENT_INTERVAL_SECONDS","0.05")))
producer.flush()
print("events published")
