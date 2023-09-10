
### Setup
**Load Data to GCS Bucket**

Load `fhv 1029` data with this script:

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    df = pd.read_csv(dataset_url, compression='gzip')
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:

    df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
    df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
    df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

    """Fix dtype issues"""
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

    # See https://pandas.pydata.org/docs/user_guide/integer_na.html
    df["PUlocationID"] = df["PUlocationID"].astype('Int64')
    df["DOlocationID"] = df["DOlocationID"].astype('Int64')

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(color: str, df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally"""
    Path(f"Data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"Data/{color}/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")

    return path


@task
def write_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    to_path = path.as_posix()
    # Using your gcs block here
    gcs_block = GcsBucket.load("your_block_name")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=to_path)
    return


@flow()
def web_to_gcs(color, year) -> None:
    for month in range(1, 13):
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(color, df_clean, dataset_file)
        write_gcs(path)

if __name__ == "__main__":
    color = "fhv"
    year = 2019
    web_to_gcs(color, year)
```
Check yout GCS Bucket to confirm that all data has been loaded.

**Create external table in BigQuery**

To make external table in BigQuery, you can use this query:
```
CREATE OR REPLACE EXTERNAL TABLE `your_project_id.your_dataset.external_fhv_2019`
OPTIONS ( 
  format = 'CSV',
  uris = ['gs://"GCS Bucket Path"/fhv_tripdata_2019-*.csv.gz']
)
```
**Create table in BigQuery**

Click the three dots on the right of your dataset, choose "Create table". We can create table like this (Change the url based on your GCS Bucket path):

![Setup1](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/7dde69c8-39df-4d31-8c79-f31eb0429dbf)

Scroll down and make sure that the table we'll create won't be  partitioned or clustered.

![Setup2](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/961006f5-d1b8-4728-bef1-76e463891dd3)

### Question 1

What is the count for fhv vehicle records for year 2019?
- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

**Solution:**

Run these queries:

```
-- External table
SELECT COUNT(*) AS total_rows 
FROM `dtc-de-395006.week_3_homework.external_fhv_2019`;

-- OR

-- BQ Table
SELECT COUNT(*) AS total_rows 
FROM `dtc-de-395006.week_3_homework.external_fhv_2019`;
```
Both of them have the same output : 

![Question 1](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/07641737-3a0e-4c43-8ce6-08dafa1d96c3)

But the query from the external table takes more time to execute.

### Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.\
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table 

**Solution:**
Run this query:

```
-- External table

SELECT COUNT(DISTINCT(affiliated_base_number)) AS total_distinct_number
FROM `dtc-de-395006.week_3_homework.external_fhv_2019`;
--- This query will process 0 MB when run.
```
And this query:

```
-- BQ table

SELECT COUNT(DISTINCT(affiliated_base_number)) AS total_distinct_number 
FROM `dtc-de-395006.week_3_homework.fhv_2019`;
--- This query will process 317.94 MB when run.
```

Both of them have the same output :

![Question 2](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/e43d5e50-b212-45ac-8d92-471a42bcbf9f)

The answer: 0 MB for the External Table and 317.94MB for the BQ Table

### Question 3:
How many records have both a blank (null) `PUlocationID` and `DOlocationID` in the entire dataset?
- 717,748
- 1,215,687
- 5
- 20,332

**Solution:**

Run this query:

```
-- External table
SELECT COUNT(DISTINCT(affiliated_base_number)) AS total_distinct_number 
FROM `dtc-de-395006.week_3_homework.external_fhv_2019`;
-- This query will process 0 MB but takes more time.

-- BQ table
SELECT COUNT(*) AS total_blank_rows 
FROM `dtc-de-395006.week_3_homework.fhv_2019`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
-- This query will process 638.9 MB when run but a lot faster.
```

The output:

![Question 3](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/60201674-d0d3-4d1e-8b4f-54874d308172)

The answer: 717,748 rows

### Question 4:
What is the best strategy to optimize the table if query always filter by `pickup_datetime` and order by `affiliated_base_number`?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

**Solution:**

To answer the question lets do partitioning and clustering based on the option.
```
-- Option 1
-- Cluster on pickup_datetime & affiliated_base_number
-- For option 1, if I cluster by DATE(pickup_datetime) an error appear: Entries in the CLUSTER BY clause must be column names. So for option 1, I used pickup_datetime without converting it to date form.

CREATE OR REPLACE TABLE `week_3_homework.fhv_2019_option_1`
CLUSTER BY pickup_datetime, Affiliated_base_number
AS (SELECT *
      FROM `week_3_homework.fhv_2019`);


-- Option 2
-- Partition on pickup_datetime & Cluster on affiliated_base_number

CREATE OR REPLACE TABLE `week_3_homework.fhv_2019_option_2`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS (SELECT *
      FROM `week_3_homework.fhv_2019`);


-- Option 3
-- Partition on pickup_datetime & affiliated_base_number
-- This option isn't possible, if we try to write the query there will be an error: Only a single PARTITION BY expression is supported but found 2.

CREATE OR REPLACE TABLE `week_3_homework.fhv_2019_option_3`
PARTITION BY pickup_datetime, Affiliated_base_number
AS (SELECT *
      FROM `week_3_homework.fhv_2019`);


-- Option 4
-- Partition by affiliated_base_number & cluster on pickup_datetime
-- This option isn't possible because data type of affiliated_base_number is STRING and it can't be partitioned.

CREATE OR REPLACE TABLE `week_3_homework.fhv_2019_option_4`
PARTITION BY Affiliated_base_number
CLUSTER BY pickup_datetime
AS (SELECT *
      FROM `week_3_homework.fhv_2019`);
```

Run the query with filter by `pickup_datetime` and order by `affiliated_base_number` to option 1 and option 2.

```
-- Option 1
-- This query will process 2.25 GB when run.
SELECT * FROM `dtc-de-395006.week_3_homework.fhv_2019_option_1`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-02-28'
ORDER BY Affiliated_base_number;

-- Option 2
-- This query will process 1.29 GB when run. 
SELECT * FROM `dtc-de-395006.week_3_homework.fhv_2019_option_2`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-02-28'
ORDER BY Affiliated_base_number;
```

The best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number is the option 2.

The answer : Partition by pickup_datetime & Cluster on Affiliated_base_number

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).\
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

**Solution:**
Run the query in the non-partitioned and partitioned table from the question 4 (option 2):

```
-- Non-partitioned
-- This query will process 647.87 MB when run. 
SELECT DISTINCT affiliated_base_number FROM `dtc-de-395006.week_3_homework.fhv_2019`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';


-- Partitioned table
-- This query will process 23.05 MB when run. 
SELECT DISTINCT affiliated_base_number FROM `dtc-de-395006.week_3_homework.fhv_2019_option_2`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

The answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

The answer: Because the source data for the created external table is a GCP Bucket, that data is located in the GCP Bucket.


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

**Solutin:**
You might consider these condition before cluster your data:
- You need a strict query cost estimate before you run a query. The cost of queries over clustered tables can only be determined after the query is run. Partitioning provides granular query cost estimates before you run a query.
- If your data is small then clustering does not offer significant performance, but cluster your data will cost more.

The answer: False

