import pandas as pd
import os
from dotenv import load_dotenv
from datetime import date
from kafka import KafkaConsumer
from minio import Minio
import json

load_dotenv()

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
ENDPOINT = "localhost:9000"

def create_consumer(bootstrap_server=['localhost:9092'], topic_name=""):
    
    # Create consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    return consumer

class Ingestion:
    def __init__(self, consumer) -> None:
        self.consumer = consumer
        
    def ingest(self):
        for message in self.consumer:
        
            data = json.loads(message.value)
            sensor_id = data['sensor_id']
            df = pd.DataFrame(data=data, index=[0])
            
            # Load to MinIO partition by year, month, day
            year = str(date.today().year)
            month = str(date.today().month)
            day = str(date.today().day)

            name = str(data.get('timestamp'))
            df.to_parquet(f"s3://datalake/{sensor_id}/year={year}/month={month}/day={day}/{name}.parquet",
                        storage_options={
                            "key": AWS_ACCESS_KEY,
                            "secret": AWS_SECRET_KEY,
                            "client_kwargs": {"endpoint_url": "http://localhost:9000/"}
                        })
            print(f"Add {name}.parquet to datalake")
            
if __name__ == "__main__":
    # Stream sensor data into MinIO partition by sensor id, day, month, year
    consumer = create_consumer(topic_name="66497c500f588f3e5549f8a6")
    Ingestion = Ingestion(consumer)
    Ingestion.ingest()