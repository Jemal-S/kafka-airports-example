from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "airports",
    bootstrap_servers="localhost:9092",
    group_id="airport-transform-group",
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

messages = []

# Collect messages first
for idx, msg in enumerate(consumer):
    a = msg.value
    a["altitude_m"] = round(a["altitude_ft"] * 0.3048, 2)
    messages.append(a)
    
    print(f"Collected -> {a['name']} ({a['altitude_m']} m)")

    # Stop after 15 messages (optional, your file size)
    if idx >= 14:
        break

# Write all messages as a proper JSON array
with open("airports_clean.json", "w", encoding="utf-8") as f:
    json.dump(messages, f, indent=4)

print("Saved all messages as a proper JSON array ✔")
