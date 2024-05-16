import json 
from kafka import KafkaConsumer


if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'messages',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    for message in consumer:
        print(json.loads(message.value))
        with open("text.txt", "w") as f:
            f.write(str(json.loads(message.value)))