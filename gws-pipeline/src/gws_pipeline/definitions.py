from dagster import Definitions
from gws_pipeline.defs.ingestion.assets import incremental_fetcher_assets, incremental_loader_assets
from gws_pipeline.defs.ingestion.resources import GoogleReportsAPIResource, StateFileResource, duckdb_motherduck
from gws_pipeline.defs.processing.assets import processing_assets
from gws_pipeline.jobs import job_all, schedule_hourly

defs = Definitions(
    assets=[*incremental_fetcher_assets, *processing_assets, *incremental_loader_assets],
    resources={
        "google_reports_api": GoogleReportsAPIResource(),
        "state_file": StateFileResource(),
        "duckdb_motherduck": duckdb_motherduck,
    },
    jobs=[job_all],
    schedules=[schedule_hourly],
)
