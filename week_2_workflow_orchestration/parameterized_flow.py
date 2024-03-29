from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=1,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def fetch(dataset_url : str) -> pd.DataFrame:
    # Read taxi data from web into pandas Dataframe    
    
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:
    # Fix dtype issues
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    # Write Dataframe out locally as parquet file
    path = Path(f"Data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_gcs(path) -> None:
    # Upload local parquet to GCS
    to_path = path.as_posix()
    gcs_block = GcsBucket.load("demo-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=to_path)
    return

@flow
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    # The main ETL function
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_cleaned = clean(df)
    path = write_local(df_cleaned, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(year: int = 2021, months: list[int] = [1,2], color: str= "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__=="__main__":
    color = "yellow"
    months = [1,2,3]
    year = 2021
    etl_parent_flow(year, months, color)