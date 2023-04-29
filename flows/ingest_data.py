import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-06.csv.gz"
if url.endswith("csv.gz"):
   csv_name = 'yellow_tripdata_2019-06.csv.gz'
else:
   csv_name = 'yellow_tripdata_2019-06.csv'
@task(log_prints=True,retries=3)
def extract_file():
    os.system(f"wget {url} -O {csv_name}")
@task(log_prints=True)
def transform_data():
    df =pd.read_csv(csv_name)
    print(f"Missing Passenger count {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count']!=0]
    print(f"Missing Passenger count {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True,retries=3)
def ingest_data(user,password,host,port,db,table_name,df):
    database_connect = SqlAlchemyConnector.load("sql-alchemy-postgres")
    engine = database_connect.get_connection(begin = False)
    start = time()
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name = table_name, con = engine, if_exists = 'replace',chunksize = 100000)
    end = time()
    total_time = end-start
    print(f"{total_time} seconds to load into db")
@flow(name='SubFlow')
def print_table(table_name):
    print(f"Table name is {table_name}")
@flow(name="Ingest Flow")
def main_flow(table_name:str):
    user = 'admin'
    password = 'admin'
    host = 'localhost'
    port = '5432'
    db = 'ny_taxi'
    print_table(table_name)
    extract_file()
    data = transform_data()
    ingest_data(user,password,host,port,db,table_name,data)
if __name__ == '__main__':
    main_flow('ny_yellow_taxi1')