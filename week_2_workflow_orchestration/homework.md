## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770

Note:  
Code
```python
@task(log_prints=True, retries=3)
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
def etl_web_to_gcs() -> None:
    """The main ETL function
    """
    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url, color)
    path = write_local(df, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
```
Results: 
rows: 447770

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`   * 
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

Note:
```
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * * <command to execute>
```

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920  *
- 12,282,990
- 27,235,753
- 11,338,483


Code:
```python
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
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("dezoom-gcp-creds")
    credential = gcp_credentials_block.get_credentials_from_service_account()

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="data-engineering-demo-375721",
        credentials=credential,
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int=2021, months: list[int]=[1,2,3], color: str="yellow") -> None:
    """The main ETL function to load data into BigQuery"""
    for month in months:
        df = extract_from_gcs(color, year, month)
        write_bq(df)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_gcs_to_bq(year, months, color)
```


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605   * 
- 190,225



## Question 5. Email notifications

The hosted Prefect Cloud lets you avoid running your own server and has automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at [app.prefect.cloud](https://app.prefect.cloud/) and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run succeeds. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see a success notification.

How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`  * 


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks on the next page of the UI (*).

- 5
- 6
- 8
- 10


## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97 
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here
