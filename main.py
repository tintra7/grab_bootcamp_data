from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import pandas as pd 
from datetime import date
from pymongo import MongoClient
from fastapi import Body, FastAPI, status
from fastapi.responses import JSONResponse
from bson import ObjectId
import os
from dotenv import load_dotenv
import uvicorn
from np_encoder import NpEncoder

load_dotenv()

URI = "mongodb://localhost:27017"

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
ENDPOINT = "localhost:9000"
app = FastAPI()

@app.get("/")
def read_root():
    return "Hello word"

# Get current sensor data (temperature and humidity)
@app.get("/api/sensor/current")
async def send_sensor(sensor_id: str):
    # Check if sensor_id is in mongo database
    print("Get current sensor data")
    client = MongoClient(URI)
    iot_db = client['iot_management']
    sensor_col = iot_db['sensors']
    sensor = sensor_col.find_one({'_id': ObjectId(sensor_id)})
    
    if not sensor:
        client.close()
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Not found sensor id"})
    client.close()
    print(sensor_id)
    consumer = KafkaConsumer(
        sensor_id,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    for message in consumer:
        response = json.loads(message.value)
        print(response)
        return JSONResponse(status_code=status.HTTP_200_OK, content=response)
    

@app.get("/api/sensor/last")
async def send_historic_value(sensor_id, record):
    record = int(record)
    # Check if sensor_id is in mongo database
    client = MongoClient(URI)
    iot_db = client['iot_management']
    sensor_col = iot_db['sensors']
    sensor = sensor_col.find_one({'_id': ObjectId(sensor_id)})

    if not sensor:
        client.close()
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Not found sensor id"})
    client.close()

    df = pd.read_parquet(f"s3://datalake/{sensor_id}",
                         storage_options={
                        "key": AWS_ACCESS_KEY,
                        "secret": AWS_SECRET_KEY,
                        "client_kwargs": {"endpoint_url": "http://localhost:9000/"}
                    })
    df = df.tail(record)
    response = {}
    print(df.columns)
    for i in df.columns:
        response[i] = list(df[i].values)
    response = json.dumps(response, cls=NpEncoder)
    return JSONResponse(status_code=status.HTTP_200_OK, content=response)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)