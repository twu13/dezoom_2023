from prefect import flow
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from google.cloud import bigquery


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
        etl_gcs_to_bq(year)


if __name__ == "__main__":
    year = [2021]
    etl_parent_flow(year)
