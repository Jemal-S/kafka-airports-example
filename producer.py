from kafka import KafkaProducer
import time, json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "airports"

with open("airports.txt", "r", encoding="utf-8") as file:
    for line in file:
        row = line.strip().split(",")

        message = {
            "id": int(row[0]),
            "name": row[1].strip('"'),
            "city": row[2].strip('"'),
            "country": row[3].strip('"'),
            "iata": row[4].strip('"'),
            "icao": row[5].strip('"'),
            "lat": float(row[6]),
            "lon": float(row[7]),
            "altitude_ft": int(row[8]),
            "timezone": row[11].strip('"'),
            "type": row[12].strip('"')
        }

        producer.send(topic, message)
        print("Sent ->", message["name"], "(", message["altitude_ft"], "ft )")
        time.sleep(1)  # Delay 1 second between messages

producer.flush()
print("Finished streaming file")