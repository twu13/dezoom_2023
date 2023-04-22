from prefect_gcp.credentials import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile, DbtCliProfile, DbtCoreOperation

your_GCS_bucket_name = ""    # (1) insert your GCS bucket name
gcs_credentials_block_name = "zoom-gcs"

credentials_block = GcpCredentials(
    service_account_info={}  # (2) enter your credentials info here
)

credentials_block.save(f"{gcs_credentials_block_name}", overwrite=True)

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load(f"{gcs_credentials_block_name}"),
    bucket=f"{your_GCS_bucket_name}",
)

bucket_block.save(f"{gcs_credentials_block_name}-bucket", overwrite=True)

credentials = GcpCredentials.load("zoom-gcp-creds")
target_configs = BigQueryTargetConfigs(
    schema="dezoomproj",  # also known as dataset
    credentials=credentials,
)
target_configs.save("cms-payments-target-config", overwrite=True)

dbt_cli_profile = DbtCliProfile(
    name="default", 
    target="dev",
    target_configs=target_configs,
)
dbt_cli_profile.save("cms-payments-cli-profile", overwrite=True)

dbt_cli_profile = DbtCliProfile.load("cms-payments-cli-profile")
dbt_core_operation = DbtCoreOperation(
    commands=["dbt run"],
    dbt_cli_profile=dbt_cli_profile,
    overwrite_profiles=True,
)
dbt_core_operation.save("cms-payments-core-op", overwrite=True)
