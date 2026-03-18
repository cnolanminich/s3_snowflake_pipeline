import dagster as dg

daily_etl_job = dg.define_asset_job(
    name="daily_etl_job",
    selection="group:ingestion+",
    description="Daily pipeline: ingestion assets and all downstream (dbt Cloud transforms, Hightouch reverse ETL)",
)


@dg.schedule(cron_schedule="0 6 * * *", target=daily_etl_job)
def daily_pipeline_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    return dg.RunRequest()
