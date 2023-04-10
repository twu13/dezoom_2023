from google.cloud import bigquery
client = bigquery.Client()
bucket_name = 'prefect_02'
project = "dtc-de-course-375205"
dataset_id = "dezoomproj"
table_id = "agg_mfc_gpo_monthly"

destination_uri = "gs://prefect_02/data/payments/agg_mfc_gpo_monthly.csv"
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
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
)