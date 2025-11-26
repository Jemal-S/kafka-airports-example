from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "airports",
    bootstrap_servers="localhost:29092",
    group_id="airport-consumer-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("\n High-altitude airports (>2000ft):\n")

for msg in consumer:
    airport = msg.value
    if airport["altitude_ft"] > 2000:
        print(f"{airport['name']} | {airport['altitude_ft']} ft | {airport['country']}")
