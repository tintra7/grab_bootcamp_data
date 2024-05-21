from fastapi import FastAPI
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from kafka import KafkaConsumer
import json
import pandas as pd 
from datetime import date
from pymongo import MongoClient

from bson import ObjectId
import os
from dotenv import load_dotenv
import uvicorn
from np_encoder import NpEncoder

load_dotenv()

URI = os.environ.get("DB_URI")

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
ENDPOINT = "localhost:9000"
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return "Hello word"

# Get current sensor data (temperature and humidity)
@app.get("/api/sensor/current")
async def send_sensor(sensorId: str):
    # Check if sensor_id is in mongo database
    print("Get current sensor data")
    client = MongoClient(URI)
    iot_db = client['test']
    sensor_col = iot_db['sensors']
    sensor = sensor_col.find_one({'_id': ObjectId(sensorId)})
    
    if not sensor:
        client.close()
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Not found sensor id"})
    client.close()
    print(sensorId)
    consumer = KafkaConsumer(
        sensorId,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    for message in consumer:
        response = json.loads(message.value)
        print(response)
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
    

@app.get("/api/sensor/last")
async def send_historic_value(sensorId, record):
    record = int(record)
    # Check if sensor_id is in mongo database
    client = MongoClient(URI)
    iot_db = client['test']
    sensor_col = iot_db['sensors']
    sensor = sensor_col.find_one({'_id': ObjectId(sensorId)})

    if not sensor:
        client.close()
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Not found sensor id"})
    client.close()


    df = pd.read_parquet(f"s3://datalake/{sensorId}",
                        storage_options={
                    "key": AWS_ACCESS_KEY,
                    "secret": AWS_SECRET_KEY,
                    "client_kwargs": {"endpoint_url": "http://localhost:9000/"}
                }, engine='fastparquet').drop(['year', 'month', 'day', 'sensor_id'], axis=1)
    df['timestamp'] = pd.to_datetime(df['timestamp'],unit='s')
    df['timestamp'] = df['timestamp'].astype(str)
    df = df.tail(record)
    response = {}
    print(df.columns)
    for i in df.columns:
        response[i] = list(df[i].values)
    response = json.dumps(response, cls=NpEncoder)
    return JSONResponse(status_code=status.HTTP_200_OK, content=json.loads(response))

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=5000)