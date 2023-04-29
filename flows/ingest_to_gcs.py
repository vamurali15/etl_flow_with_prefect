from pathlib import Path
import pandas as pd
from prefect import task,flow,get_run_logger
from prefect_gcp import GcsBucket
from prefect_gcp import GcpCredentials
import os
@task(name = 'extract task',retries = 2)
def extract(url) -> pd.DataFrame:
    #df = pd.read_csv(f"{color}_tripdata_{year}-{month}.csv.gz")
    df = pd.read_csv(url)
    print(df.dtypes)
    return df
@task(name = 'clean_data')
def transform(df:pd.DataFrame) -> pd.DataFrame:
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df
@task(name= "Write to local")
def write_to_local(df:pd.DataFrame,color:str,dataset_name:str) -> None:
    path = f"data/{color}/"
    file_name = f"{dataset_name}.parquet"
    if not os.path.exists(path):
        os.system(f"mkdir -p {path}")
    full_file = os.path.join(path,file_name)
    df.to_parquet(full_file,compression='gzip')      
    return Path(full_file)

@task(name = "write to GCS")
def write_to_gcp(full_file:Path) -> None:
    gcp_block = GcsBucket.load("gcs-credentials")
    gcp_block.upload_from_path(from_path = full_file,to_path = full_file)
    return
@flow(name= "Write_to_GCS")
def write_to_gcs(color,month,year):
    #url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz"
    dataset = f"{color}_tripdata_{year}-{month}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset}.csv.gz"
    #df = extract(color,year,month)
    df = extract(url)
    df = transform(df)
    full_file = write_to_local(df,color,dataset_name=dataset)
    write_to_gcp(full_file)
    
@flow(name = "New_Parent_Flow")
def new_parent_flow(color,month,year):
    write_to_gcs(color,month,year)
    
if __name__ == "__main__":
    new_parent_flow(color,month,year)
