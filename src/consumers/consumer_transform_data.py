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

messages = []

print("Collecting messages... Press Ctrl+C when done.\n")

try:
    # Collect messages first
    for idx, msg in enumerate(consumer):
        a = msg.value
        if a is not None:
            a["altitude_m"] = round(a["altitude_ft"] * 0.3048, 2)
            messages.append(a)
            
            print(f"Collected -> {a['name']} ({a['altitude_m']} m)")
except KeyboardInterrupt:
    print(f"\n\nStopped collecting. Total messages: {len(messages)}")

# Write all messages as a proper JSON array
if messages:
    with open("../data/airports_clean.json", "w", encoding="utf-8") as f:
        json.dump(messages, f, indent=4)
    
    print(f"✔ Saved {len(messages)} messages to airports_clean.json")
else:
    print("No messages to save.")

consumer.close()