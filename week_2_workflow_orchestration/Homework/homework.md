
## Question 1

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

#### Solution:

Script:
```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url : str) -> pd.DataFrame: 
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:

    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    path = Path(f"Data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_gcs(path) -> None:
    to_path = path.as_posix()
    gcs_block = GcsBucket.load("demo-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=to_path)
    return

@flow
def etl_web_to_gcs() -> None:
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_cleaned = clean(df)
    path = write_local(df_cleaned, color, dataset_file)
    write_gcs(path)

if __name__=="__main__":
    etl_web_to_gcs()
```
One of the output after I ran the script:
```
| INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
```
The answer: 447,770 rows

## Question 2

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

#### Solution:

Cron expression consist of five parts that set the time for task execution. These parts are:
- Minutes (0-59)
- Hours (0-23)
- Day of the Month (1-31)
- Month (1-12 or Jan-Dec)
- Day of the Week (0-6 or Sun-Sat)

Set a cron schedule can be done in two ways:
- By writing it in the deployment script :
```
prefect deployment build "path to etl_web_to_gcs.py":etl_web_to_gcs -n "etl web to gcs" --cron "0 5 1 * *" -a
```
Output:

![Jawaban No  2A](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/1b13158a-ad59-4fb6-81e5-c361e818d1e1)

In the Orion UI should be display like this:
![Jawaban No  2B](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/e25268f3-e54c-4694-9013-046efcc26da4)

- By set it in the Orion UI:
    - First, deploy etl_web_to_gcs script
        ```
        prefect deployment build "path to etl_web_to_gcs.py":etl_web_to_gcs -n "etl web to gcs" -a
       ```
    - And set the schedule in the Orion UI
      
      ![Jawaban No  2](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/c48711fe-a083-4a3b-ab6d-351a3aa391ad)

The answer : `0 5 1 * *`

## Question 3

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery:
- This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).
- The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
- Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 
- Make any other necessary changes to the code for it to function as required.
- Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).
- Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. 
- Run your deployment to append this data to your BiqQuery table. 
How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483

#### Solution

**Step 1:**
To load Yellow Taxi Data (Feb. 2019 & March 2019), I used `parameterized_flow.py` and modified the parameter. If you deploy that script before, you can run in Orion UI with custom run and edit the parameter. 

`parameterized_flow.py` :
```
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
    color = "green"
    months = [2,3]
    year = 2019
    etl_parent_flow(year, months, color)

```

**Step 2:** Edit `etl_gcs_to_bq` to extract & load (just extract & load, no transform) Yellow taxi data for Feb. 2019 and March 2019.

`etl_gcs_to_bq` :
```
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
```
**Step 3:** Deploy `etl_gcs_to_bq` with this command:
```
prefect deployment build etl_gcs_to_bq.py:parent_flow -n etl_gcs_to_bq -a
```
start an agent to run flow from this deployment:
```
prefect agent start -q 'default'
```
Since I didn't set the default parameter, I have to set the parameter in Orion UI when I run the flow (choose custom run):

![Jawaban No  3A](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/5eaf1ce7-b3c3-4c89-af59-94ac3d062290)


Logs

![Jawaban No  3B](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/3b77704b-b998-4f07-928c-21bd955add28)

The answer = 14,851,920 rows

## Question 4

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225

#### Solution
For this question I tried so many times but it didn't work. There is an error like this:
```
Script at './week_2_workflow_orchestration/Homework/etl_web_to_gcs_homework_github.py' encountered an exception: FileNotFoundError(2, 'No such file or 
directory')
```

Sometime the deployment works, but if I run the flow there is an error like this:

```
prefect.exceptions.ScriptError: Script at 'etl_web_to_gcs.py' encountered an exception: FileNotFoundError(2, 'No such file or directory')
```
PLease contact me if you have solution for my problem, I would be very happy. 

### Question 5

It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 

How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`

#### Solution

**First**, Login to app.prefect.cloud and create API key. And run this command to signin in your cli:

```
prefect cloud login -k your_IP_KEY
```

**Second**, deploy the workflow. For workflow I used "link" with some edit in variable month, year, and color.
Run this command:

```
prefect deployment build build etl_web_to_gcs.py:etl_web_to_gcs -n with_notification -a
```

**Third**, In your prefect cloud create email block (for this question I used email instead slack). Then, go to _Automation > Add Automation_.

![6A](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/0fd29d37-57aa-40e6-8b67-42d8290e9d53)

![6B](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/02cb4f4c-fd8e-45d1-8ab0-e1ae0680b078)

**Finally**, Run the flow, you can run through cli or prefect cloud. I used prefect cloud to run the flow.

Ouput:
![6](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/7571f47f-aa4d-499d-a990-820dae85db4f)

Notification Email:
![6C](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/16e7db62-0614-45f0-ad33-0a1090e64947)

Flow failed to upload to gcs because I didn't create gcs block like in the Prefect Orion. 

The answer: 514,392
