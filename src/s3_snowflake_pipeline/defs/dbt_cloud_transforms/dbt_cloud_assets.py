import os

import dagster as dg
from dagster_dbt import DbtCloudCredentials, DbtCloudWorkspace, dbt_cloud_assets

dbt_cloud_credentials = DbtCloudCredentials(
    account_id=int(os.environ.get("DBT_CLOUD_ACCOUNT_ID", "0")),
    token=os.environ.get("DBT_CLOUD_API_TOKEN", ""),
    access_url=os.environ.get("DBT_CLOUD_ACCESS_URL", "https://cloud.getdbt.com"),
)

dbt_cloud_workspace = DbtCloudWorkspace(
    credentials=dbt_cloud_credentials,
    project_id=dg.EnvVar.int("DBT_CLOUD_PROJECT_ID"),
    environment_id=dg.EnvVar.int("DBT_CLOUD_ENVIRONMENT_ID"),
)


@dbt_cloud_assets(workspace=dbt_cloud_workspace)
def my_dbt_cloud_assets(context: dg.AssetExecutionContext, dbt_cloud: DbtCloudWorkspace):
    yield from dbt_cloud.cli(["build"], context=context).stream()
