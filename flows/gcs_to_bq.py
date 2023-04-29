from pathlib import Path
import pandas as pd
from prefect import task,flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(name = "Extract from GCS")
def extract_from_gcs(color:str, month:str, year:int)-> Path:
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month}.parquet"
    gcp_block = GcsBucket.load("gcs-credentials")
    gcp_block.get_directory(gcs_path, "./data/" )
    return Path(f"./data/{gcs_path}")
@task(name = "Transform data")
def transform_data(path):
    df = pd.read_parquet(path)
    print(f"Number of Missing Passengers {df['passenger_count'].isna().sum()}")
    df.fillna(0,inplace=True)
    print(f"Number of Missing Passengers {df['passenger_count'].isna().sum()}")
    print(df.shape)
    return df
@task(name = "Write to Big Query")
def write_to_gbq(df:pd.DataFrame) ->None:
     gcp_block = GcsBucket.load("gcs-credentials")
     gcp_credentials = GcpCredentials.load("gcs-credentials")
     df.to_gbq(
         destination_table = "dezoomcamp.yellow_taxi_data",
         project_id= "dezoomcamp-383614",
         credentials= gcp_credentials.get_credentials_from_service_account(),
         chunksize= 250_000,
         if_exists= "append"

     )

@flow
def gcs_to_bq_etl():
    color = 'yellow'
    month = '01'
    year = 2019
    path = extract_from_gcs(color=color,month=month,year=year)
    df = transform_data(path)
    write_to_gbq(df)
if __name__ == "__main__":
    gcs_to_bq_etl()
    