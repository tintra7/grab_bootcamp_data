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


class Ingestion:
    def __init__(self, bootstrap_server, topic_name) -> None:
        self.bootstrap_server = bootstrap_server
        self.topic_name = topic_name
        
    def create_consumer(self):
        
        # Create consumer
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_server,
            auto_offset_reset='latest',
            enable_auto_commit='true'
        )
        return consumer
        
    def ingest(self):
        consumer = self.create_consumer()
        for message in consumer:
        
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
    Ingestion = Ingestion("localhost:9092", "test1")
    Ingestion.ingest()