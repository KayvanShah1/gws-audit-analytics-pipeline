from dagster import AssetSelection, ScheduleDefinition, define_asset_job

job_all = define_asset_job(
    name="gws_audit_job",
    selection=AssetSelection.groups("raw_fetch", "light_transform", "warehouse_publish"),
    description="Google Workspace Audit Job",
)
schedule_hourly = ScheduleDefinition(job=job_all, cron_schedule="0 6 */2 * *")
