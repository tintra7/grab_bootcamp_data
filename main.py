from fastapi import FastAPI
from kafka import KafkaConsumer
import json

app = FastAPI()

@app.get("/")
def read_root():
    return "Hello word"

@app.get("/api/sensor/{user_id}")
def send_sensor(user_id: str):
    consumer = KafkaConsumer(
        user_id,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    for message in consumer:
        print(json.loads(message.value))
        return json.loads(message.value)

