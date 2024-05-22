from fastapi import FastAPI
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from kafka import KafkaConsumer
import json
import pandas as pd 
from datetime import date, timedelta
from pymongo import MongoClient

from bson import ObjectId
import os
from dotenv import load_dotenv
import uvicorn
from np_encoder import NpEncoder

from minio import Minio
from io import BytesIO

load_dotenv()

URI = os.environ.get("DB_URI")

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
ENDPOINT = "localhost:9000"

topic = "sensor"
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


client = Minio(
    ENDPOINT,
    access_key=AWS_ACCESS_KEY,
    secret_key=AWS_SECRET_KEY,
    secure=False
    )

def get_list(sensorId, record):
    count = 0
    # List object of today
    today = date.today()
    year = today.year,
    month= today.month, 
    day= today.day
    
    if isinstance(year, tuple):
        year = year[0]
    if isinstance(month, tuple):
        month = month[0]
    if isinstance(day, tuple):
        day = day[0]
    
    name_list = []
    print(month)
    objects = client.list_objects("datalake", f"{sensorId}/year={year}/month={month}/day={day}/")
    for obj in objects:
        name_list.append(obj.object_name)
    count += len(name_list)
    # If number of object today smaller than number of record then read object of yester and so on
    while len(name_list) < record:
        yesterday = today - timedelta(days = 1)
        year=yesterday.year, 
        month=yesterday.month, 
        day=yesterday.day
        if isinstance(year, tuple):
            year = year[0]
        if isinstance(month, tuple):
            month = month[0]
        if isinstance(day, tuple):
            day = day[0]
        objects = client.list_objects("datalake", f"{sensorId}/year={year}/month={month}/day={day}/")
        for obj in objects:
            name_list.append(obj.object_name)
    return name_list[-record:]

def read_data(name_list):
    df = pd.DataFrame()
    for name in name_list:
        data = client.get_object("datalake", name)
        data = data.read()
        data = pd.read_parquet(BytesIO(data))
        df = pd.concat([df, data])
        
    df['timestamp'] = pd.to_datetime(df['timestamp'],unit='s')
    df['timestamp'] = df['timestamp'].astype(str)
    return df


@app.get("/")
def read_root():
    return "Hello word"

# Get current sensor data (temperature and humidity)
@app.get("/api/sensor/current")
async def send_sensor(sensorId: str):
    # Check if sensor_id is in mongo database
    print("Get current sensor data")
    try:
        
        client = MongoClient(URI)
        iot_db = client['test']
        sensor_col = iot_db['sensors']
        sensor = sensor_col.find_one({'_id': ObjectId(sensorId)})
    except:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Connect to database fail!"})
    if not sensor:
        client.close()
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"message": "Not found sensor id"})
    client.close()
    print(sensorId)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit='true'
    )
    for message in consumer:
        response = json.loads(message.value)
        if response.get("sensor_id") == sensorId:
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


    name_list = get_list(sensorId=sensorId, record=record)
    df = read_data(name_list=name_list)
    response = {}
    print(df.columns)
    for i in df.columns:
        response[i] = list(df[i].values)
    response = json.dumps(response, cls=NpEncoder)
    return JSONResponse(status_code=status.HTTP_200_OK, content=json.loads(response))

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=5000)