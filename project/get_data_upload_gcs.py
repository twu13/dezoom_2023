from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from google.cloud import bigquery
import os

# 2021 https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv
# 2020 https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv
# 2019 https://download.cms.gov/openpayments/PGYR19_P012023/OP_DTL_GNRL_PGYR2019_P01202023.csv

# Define the date parser function to convert the date string to a pandas datetime object
date_parser = lambda x: pd.to_datetime(x, format="%m/%d/%Y")


@task(retries=3)
def fetch(dataset_url: str, dataset_file: str, year: int) -> Path:
    """Read payments data from web, break into monthly datasets, and save into data/payments/"""
    path_to_download = Path(f"data/payments/{dataset_file}.csv")
    if not os.path.exists(path_to_download):
        os.system(f"wget {dataset_url} -P data/payments/")
    else:
        print("File exists already, skipping the download")
    output_path = Path(f"data/payments/{year}/")
    chunk_size = 1000000  # read 1 million rows at a time
    file_paths = []
    for i, df_chunk in enumerate(
        pd.read_csv(
            path_to_download,
            parse_dates=["Date_of_Payment"],
            date_parser=date_parser,
            chunksize=chunk_size,
        )
    ):
        # make directory if needed
        output_path.mkdir(parents=True, exist_ok=True)
        for column in df_chunk.columns:
            if column != "Date_of_Payment":
                df_chunk[column] = df_chunk[column].astype(str)
        parquet_file_path = output_path.joinpath(f"part_{i:02}-{year}.parquet")
        df_chunk.to_parquet(parquet_file_path, index=False)
        file_paths.append(parquet_file_path)
        print(f"Wrote data for chunk {i}")

    return file_paths


@task()
def write_gcs(file_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=file_path, to_path=file_path)
    return


@flow()
def etl_web_to_gcs(year: int) -> None:
    """The main ETL function for pulling data from web and writing to GCS"""
    dataset_file = f"OP_DTL_GNRL_PGYR{year}_P01202023"
    dataset_url = f"https://download.cms.gov/openpayments/PGYR{str(year)[-2:]}_P012023/OP_DTL_GNRL_PGYR{year}_P01202023.csv"
    file_paths = fetch(dataset_url, dataset_file, year)
    for file_path in file_paths:
        write_gcs(file_path)


@flow()
def etl_gcs_to_bq(year: int) -> None:
    """The main ETL function for creating an external table in BQ from GCS"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    external_table_options = bigquery.ExternalConfig("PARQUET")
    external_table_options.autodetect = True
    external_table_options.source_uris = [f"gs://prefect_02/data/payments/{year}/*"]

    result = bigquery_create_table(
        dataset="dezoomproj",
        table=f"payments_{year}",
        external_config=external_table_options,
        gcp_credentials=gcp_credentials_block,
    )
    return result


@flow()
def etl_parent_flow(years: list[int] = 2021):
    for year in years:  # download payments data for each year
        etl_web_to_gcs(year) # write payments data as chunked parquet files to GCS
        etl_gcs_to_bq(year) # create an external table per year in BQ


if __name__ == "__main__":
    year = [2019, 2020, 2021]
    etl_parent_flow(year)
