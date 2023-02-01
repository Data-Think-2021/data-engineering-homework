#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, color) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    if color == "green":
        date_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
    elif color == "yellow":
        date_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    df = pd.read_csv(dataset_url, parse_dates = date_cols)
    #print(df.dtypes)
    #print(df.head(2))
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True, retries=3)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)  
    df.to_parquet(path, compression="gzip")

    return path

@task(log_prints=True, retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_bucket_block = GcsBucket.load("dezoom-gcs")
    gcp_bucket_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url, color)
    path = write_local(df, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(year: int=2021, months: list[int]=[1,2,3], color: str="yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "green"
    months = [2, 3]
    year = 2019
    etl_parent_flow(year, months, color)