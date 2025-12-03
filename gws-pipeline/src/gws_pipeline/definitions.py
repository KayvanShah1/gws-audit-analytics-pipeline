from dagster import Definitions
from gws_pipeline.defs.ingestion.assets import incremental_assets
from gws_pipeline.defs.ingestion.resources import GoogleReportsAPIResource, StateFileResource
from gws_pipeline.defs.processing.assets import process_events
from gws_pipeline.jobs import job_all, schedule_hourly

defs = Definitions(
    assets=[*incremental_assets, process_events],
    resources={
        "google_reports_api": GoogleReportsAPIResource(),
        "state_file": StateFileResource(),
    },
    jobs=[job_all],
    schedules=[schedule_hourly],
)
