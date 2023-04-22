from google.cloud import bigquery

datasets = ["agg_mfc_gpo_monthly",
                           "agg_nature_pay_monthly",
                           "agg_phys_monthly",
                           "agg_state_monthly"]

client = bigquery.Client()
bucket_name = 'prefect_02'
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
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
