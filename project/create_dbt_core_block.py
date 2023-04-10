from prefect_gcp.credentials import GcpCredentials
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile, DbtCliProfile, DbtCoreOperation

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