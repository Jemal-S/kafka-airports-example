from kafka import KafkaConsumer
import json

def deserialize_json(v):
    """Safely deserialize JSON messages"""
    if not v:
        return None
    try:
        return json.loads(v.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Warning: Failed to deserialize message: {e}")
        return None

consumer = KafkaConsumer(
    "airports",
    bootstrap_servers="localhost:9092",
    group_id=None,  # Don't track offsets - always read new messages only
    auto_offset_reset="latest",
    enable_auto_commit=False,
    value_deserializer=deserialize_json
)

print("\n High-altitude airports (>2000ft):\n")

try:
    for msg in consumer:
        airport = msg.value
        if airport and airport.get("altitude_ft", 0) > 2000:
            print(f"{airport['name']} | {airport['altitude_ft']} ft | {airport['country']}")
except KeyboardInterrupt:
    print("\n\nConsumer stopped.")
    consumer.close()

