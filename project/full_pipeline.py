from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from prefect_dbt.cli.commands import DbtCoreOperation
from google.cloud import bigquery
import os

# 2021 https://download.cms.gov/openpayments/PGYR21_P012023/OP_DTL_GNRL_PGYR2021_P01202023.csv
# 2020 https://download.cms.gov/openpayments/PGYR20_P012023/OP_DTL_GNRL_PGYR2020_P01202023.csv
# 2019 https://download.cms.gov/openpayments/PGYR19_P012023/OP_DTL_GNRL_PGYR2019_P01202023.csv

# Define the date parser function to convert the date string to a pandas datetime object
date_parser = lambda x: pd.to_datetime(x, format="%m/%d/%Y")


@task(retries=3)
def fetch(dataset_url: str, dataset_file: str, year: int) -> Path:
    """Read payments data from the web, break it into chunks of 1M rows, and save them data/payments/"""
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
    """Upload local parquet files to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=file_path, to_path=file_path)
    return


@flow()
def etl_web_to_gcs(year: int) -> None:
    """The main ETL function for pulling data from the web and writing to GCS"""
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


@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt run --var 'is_test_run: false'"],
        project_dir="/Users/tonywu/github/dezoom_2023/project/dbt/",
        profiles_dir="/Users/tonywu/github/dezoom_2023/project/dbt/",
    ).run()
    return result


@flow
def etl_bq_to_gcs_csv(datasets: list[str]) -> None:
    """The main ETL function for writing the analytic tables back to GCS as CSV files"""
    client = bigquery.Client()
    bucket_name = "prefect_02"
    project = "dtc-de-course-375205"
    dataset_id = "dezoomproj"

    for dataset in datasets:
        table_id = dataset

        destination_uri = f"gs://prefect_02/data/payments/{table_id}.csv"
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location="US",
        )  # API request
        extract_job.result()  # Waits for job to complete.

        print(
            "Exported {}:{}.{} to {}".format(
                project, dataset_id, table_id, destination_uri
            )
        )


@flow()
def etl_parent_flow(years: list[int] = 2021):
    for year in years:  # download payments data for each year
        etl_web_to_gcs(year)  # write payments data as chunked parquet files to GCS
        etl_gcs_to_bq(year)  # create an external table per year in BQ
        trigger_dbt_flow()  # run dbt jobs to create analytic tables
        etl_bq_to_gcs_csv(
            [
                "agg_mfc_gpo_monthly",
                "agg_nature_pay_monthly",
                "agg_phys_monthly",
                "agg_state_monthly",
            ]
        )  # write csv file back to GCS


if __name__ == "__main__":
    years = [2019, 2020, 2021]
    etl_parent_flow(years)
