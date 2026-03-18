import os

import dlt
from dlt.sources.filesystem import filesystem


# -- Shared pipeline: all S3 sources land in the same Snowflake dataset --------

s3_to_snowflake_pipeline = dlt.pipeline(
    pipeline_name="s3_to_snowflake",
    destination="snowflake",
    dataset_name="raw_data",
)


# -- Helper: create a filesystem source for a given S3 prefix + glob ----------
#
# To add a NEW S3-to-Snowflake ingestion, add one line to defs.yaml and
# one source variable here. For example:
#
#   orders_source = s3_source("orders/", "**/*.parquet")
#
# Then reference it in defs.yaml:
#
#   - source: .loads.orders_source
#     pipeline: .loads.s3_to_snowflake_pipeline

def s3_source(prefix: str, file_glob: str = "**/*.parquet"):
    bucket = os.environ.get("S3_BUCKET_URL", "s3://your-bucket")
    return filesystem(
        bucket_url=f"{bucket}/{prefix}",
        file_glob=file_glob,
    )


# -- Define one source per S3 prefix you want to ingest -----------------------

customers_source = s3_source("customers/")
transactions_source = s3_source("transactions/")
