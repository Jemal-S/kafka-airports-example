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
    group_id="airport-print-consumer-group",
    auto_offset_reset="latest", # Start reading at the latest message
    enable_auto_commit=False,
    value_deserializer=deserialize_json
)

print("\n Receiving live airport stream:\n")

try:
    for msg in consumer:
        if msg.value is not None:
            print(msg.value)
except KeyboardInterrupt:
    print("\n\nConsumer stopped.")
    consumer.close()
