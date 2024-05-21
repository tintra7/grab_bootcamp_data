import pandas as pd
import numpy as np
from pymongo import MongoClient
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import date

load_dotenv()

DB_URI = os.environ.get("DB_URI")
DB_NAME = "test"
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
ENDPOINT = "localhost:9000"

def put_to_datelake(db, table_name, 
                    year=date.today().year, 
                    month=date.today().month, 
                    day=date.today().day):
    mycol = db[table_name]
    df = pd.DataFrame(list(mycol.find()))
    print(df.head())
    # df.to_parquet(f"s3://datalake/{table_name}/year={year}/month={month}/day={day}/{table_name}.parquet",
    #                     storage_options={
    #                         "key": AWS_ACCESS_KEY,
    #                         "secret": AWS_SECRET_KEY,
    #                         "client_kwargs": {"endpoint_url": "http://localhost:9000/"}
    #                     })
    # print(f"Add {table_name}.parquet to datalake")

    

if __name__ == "__main__":
    client = MongoClient(DB_URI)
    
    mydb = client[DB_NAME]
    col_list = mydb.list_collection_names()
    for i in col_list:
        print(i)
        put_to_datelake(mydb, i)
    client.close()
