import random
from kafka import KafkaProducer
import time
import json
from time import sleep


def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

while True:
    sensor_id = f"test{random.randint(0, 5)}"
    msg = {
        "h1": random.randint(30, 60),
        "t1": random.randint(30, 37),
        "sensor_id": "test1",
        "timestamp": int(time.time())
    }
    print("Send", str(msg))
    producer.send("test1", msg)
    sleep(1)