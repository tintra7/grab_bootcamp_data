import random
from kafka import KafkaProducer
import time
import json
from time import sleep

sensor_id = "6642237e64e4a690113f5f97"

TIME_ZONE = 3600*7

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

while True:
    
    msg = {
        "humidity": random.randint(30, 60),
        "temp": random.randint(34, 37),
        "sensor_id": sensor_id,
        "timestamp": int(time.time()) + TIME_ZONE
    }
    print("Send", str(msg))
    producer.send(sensor_id, msg)
    sleep(1)