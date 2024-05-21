import requests
from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
import json
from datetime import datetime

load_dotenv()

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
sensor_id = "6642237e64e4a690113f5f97"

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    requests.get(url)

if __name__ == "__main__":
    # url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"

    consumer = KafkaConsumer(
        sensor_id,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    for message in consumer:
        response = json.loads(message.value)
        temp = response.get("temp")
        
        if temp is not None and temp >= 36:  # Assuming 100 is the threshold for overheating
            print(response)
            timestamp = response.get("timestamp")
            timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            overheat_message = f"ALERT: Temperature is {temp}°C at {timestamp}. Overheating detected!"
            send_telegram_message(overheat_message)

        
