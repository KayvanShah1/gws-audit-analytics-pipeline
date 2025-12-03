from dagster import AssetSelection, ScheduleDefinition, define_asset_job

job_all = define_asset_job(
    "gws_pipeline_job",
    selection=AssetSelection.keys("process_events"),
)
schedule_hourly = ScheduleDefinition(job=job_all, cron_schedule="0 6 */2 * *")
