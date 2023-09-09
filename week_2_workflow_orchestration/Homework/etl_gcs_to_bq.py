from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month:int) -> pd.DataFrame:
    gcs_path = f"Data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("demo-gcs")
    gcs_block.get_directory(from_path=gcs_path)
    path = Path(f"{gcs_path}").as_posix()

    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("prefect-demo")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-395006",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

@flow()
def etl_gcs_to_bq(color: str, year: int, month: int):

    df = extract_from_gcs(color, year, month)
    write_bq(df)
    
    return df

@flow(log_prints=True)
def parent_flow(color, year, months):
    rows = 0
    for month in months:
        df = etl_gcs_to_bq(color, year, month)
        rows += len(df)
    print(f"Total rows: {rows}")

if __name__=="__main__":
    color = 'yellow'
    year = 2019
    months = [2,3]
    parent_flow(color, year, months)