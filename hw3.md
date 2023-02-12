## Code for uploading FHV datasets to GCS
```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import os


@task(retries=3)
def fetch(dataset_url: str, dataset_file: str) -> Path:
    """Read fhv data from web and save into data/fhv"""
    os.system(f"wget {dataset_url} -P data/fhv/")
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    path = fetch(dataset_url, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(months: list[int] = list(range(1, 13)), year: int = 2019):
    for month in months:
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    months = list(range(1, 13))
    year = 2019
    etl_parent_flow(months, year)
```
## Code for creating external table in BQ
```
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.fhv_data` OPTIONS (
        format = 'CSV',
        uris = ['gs://prefect_02/data/fhv/fhv_tripdata_2019-*.csv.gz']
    );
```

## Code for creating internal table in BQ
```
CREATE OR REPLACE TABLE dezoomcamp.fhv_data_internal_nonp AS
SELECT *
FROM `dezoomcamp.fhv_data`;
```

## Question 1
```
SELECT COUNT(*)
COUNT `dezoomcamp.fhv_data`;
```
There are `43,244,696` fhv vehicle records for year 2019

## Question 2
```
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `dezoomcamp.fhv_data`;
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `dezoomcamp.fhv_data_internal_nonp`;
```
These queries will process `0 MB` for the External Table and `317.94 MB` for the BQ Table

## Question 3
```
SELECT COUNT(*)
FROM dezoomcamp.fhv_data_internal_nonp
WHERE PUlocationID IS NULL
    AND DOlocationID IS NULL;
```
`717,748` records have both a blank `PUlocationID` and `DOlocationID`

## Question 4
The best optimization strategy would be to `Partition by pickup_datetime Cluster on affiliated_base_number`

## Question 5
Code for creating the partitioned and clustered table:  
```
CREATE OR REPLACE TABLE dezoomcamp.fhv_data_internal_p
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT *
FROM `dezoomcamp.fhv_data`;
```
Code for selecting distinct records for both tables:
```
SELECT DISTINCT(Affiliated_base_number)
FROM dezoomcamp.fhv_data_internal_nonp
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT(Affiliated_base_number)
FROM dezoomcamp.fhv_data_internal_p
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```
These queries will process `23.06 MB` for the partitioned table and `647.87 MB` for the non-partitioned table

## Question 6
`GCP Bucket`

## Question 7
`FALSE`