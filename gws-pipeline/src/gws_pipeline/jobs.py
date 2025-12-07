from dagster import (
    AssetSelection,
    DagsterRunStatus,
    RunRequest,
    RunStatusSensorContext,
    ScheduleDefinition,
    define_asset_job,
    run_status_sensor,
)

INGESTION_GROUPS = ("raw_fetch", "light_transform", "warehouse_publish")
ANALYTICS_GROUPS = ("staging", "marts")


job_ingestion = define_asset_job(
    name="gws_ingestion_job",
    selection=AssetSelection.groups(*INGESTION_GROUPS),
    description="Fetches, processes, and loads Google Workspace audit activity into DuckDB.",
)

job_build_dbt_models = define_asset_job(
    name="job_build_dbt_models",
    selection=AssetSelection.groups(*ANALYTICS_GROUPS),
    description="Builds analytics models (dbt) on top of the audit warehouse.",
)

schedule_ingestion_every_2d = ScheduleDefinition(job=job_ingestion, cron_schedule="0 6 */2 * *")


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[job_ingestion],
    request_jobs=[job_build_dbt_models],
    name="trigger_dbt_build_models_after_ingestion",
    minimum_interval_seconds=5,
)
def trigger_dbt_build_models_after_ingestion(context: RunStatusSensorContext):
    return RunRequest(
        run_key=f"analytics_after_{context.dagster_run.run_id}",
        tags={"source_run_id": context.dagster_run.run_id},
    )
