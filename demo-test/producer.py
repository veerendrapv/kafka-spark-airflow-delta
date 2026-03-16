from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "demo-topic"

for i in range(10):
    message = {
        "id": i,
        "message": "Hello I am Veerendra, I am sending messages"
    }

    producer.send(topic, message)
    producer.flush()

    print("sent:", message)

    time.sleep(1)

print("done")