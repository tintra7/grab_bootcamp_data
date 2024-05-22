import requests
from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
import json
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId

load_dotenv()

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
URI = os.environ.get("DB_URI")
sensor_id = "sensor"

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    requests.get(url)



if __name__ == "__main__":
    # url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    try:    
        client = MongoClient(URI)
        iot_db = client['test']
        sensor_col = iot_db['rooms']

    except:
        print("Connect to database fail")

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
            sensorId = response.get("sensor_id")    

            sensor = sensor_col.find_one({'sensorId': ObjectId(sensorId)})
            room_name = sensor.get("name")
            print(response)
            timestamp = response.get("timestamp")
            timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            overheat_message = f"ALERT: Temperature of {room_name} is {temp}°C at {timestamp}. Overheating detected!"
            send_telegram_message(overheat_message)

    client.close()
