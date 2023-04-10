from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt run"],
        project_dir="/Users/tonywu/github/dezoom_2023/project/dbt/",
        profiles_dir="/Users/tonywu/github/dezoom_2023/project/dbt/"
    ).run()
    return result

trigger_dbt_flow()