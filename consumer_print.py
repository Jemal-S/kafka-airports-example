from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "airports",
    bootstrap_servers="localhost:9092",
    group_id="airport-consumer-group",  # required for offset tracking
    auto_offset_reset="latest",        # read from start if no committed offset
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("\n Receiving live airport stream:\n")

for msg in consumer:
    print(msg.value)