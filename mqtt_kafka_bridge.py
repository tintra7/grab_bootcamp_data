import ssl
import paho.mqtt.client as paho
import paho.mqtt.subscribe as subscribe
from paho import mqtt
from dotenv import load_dotenv
import os
from datetime import datetime
import json
from time import strftime, localtime, time
from kafka import KafkaProducer
# Kafka Producer

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)


load_dotenv()

USER = os.environ.get("MQTT_USER")
PASSWORD = os.environ.get("MQTT_PASS")
CLUSTER = os.environ.get("MQTT_CLUSTER")
PORT = 8883

def on_message(client, userdata, message):
    now = datetime.now()

    current_time = time()
    print("Current Time =", current_time)
    data = str(message.payload)[1:]
    print(data)
    producer.send('test1', data)



if __name__ == "__main__":
    sslSettings = ssl.SSLContext(mqtt.client.ssl.PROTOCOL_TLS)

    # put in your cluster credentials and hostname
    auth = {'username': USER, 'password': PASSWORD}
    subscribe.callback(on_message, "#", hostname=CLUSTER, port=8883, auth=auth,
                    tls=sslSettings, protocol=paho.MQTTv31)


