#!/usr/bin/env python
# coding: utf-8
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3) # , cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_bucket_block = GcsBucket.load("dezoom-gcs")
    gcp_bucket_block.get_directory(
        from_path=gcs_path
    )
    df = pd.read_parquet(f"data-engineering-demo/{gcs_path}")
    return df

@task(log_prints=True, retries=3)
def write_bq(df: pd.DataFrame, color) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("dezoom-gcp-creds")
    credential = gcp_credentials_block.get_credentials_from_service_account()

    df.to_gbq(
        destination_table=f"dezoomcamp.{color}_taxi_rides",
        project_id="data-engineering-demo-375721",
        credentials=credential,
        # chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(years: list[int], months: list[int]=[1,2,3], color: str="yellow") -> None:
    """The main ETL function to load data into BigQuery"""
    for year in years:
        for month in months:
            df = extract_from_gcs(color, year, month)
            write_bq(df, color)


if __name__ == "__main__":
    color = "green"
    years = [2019, 2020]
    months = list(range(1, 13))
    etl_gcs_to_bq(years, months, color)