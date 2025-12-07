from dagster import Definitions

from gws_pipeline.defs.analytics.assets import dbt_models
from gws_pipeline.defs.ingestion.assets import incremental_fetcher_assets, incremental_loader_assets
from gws_pipeline.defs.ingestion.resources import GoogleReportsAPIResource, StateFileResource, duckdb_motherduck
from gws_pipeline.defs.processing.assets import processing_assets
from gws_pipeline.jobs import (
    job_ingestion,
    job_build_dbt_models,
    schedule_ingestion_every_2d,
    trigger_dbt_build_models_after_ingestion,
)

from gws_pipeline.defs.analytics.resources import dbt_resource

defs = Definitions(
    assets=[
        *incremental_fetcher_assets,
        *processing_assets,
        *incremental_loader_assets,
        dbt_models,
    ],
    resources={
        "google_reports_api": GoogleReportsAPIResource(),
        "state_file": StateFileResource(),
        "duckdb_motherduck": duckdb_motherduck,
        "dbt": dbt_resource,
    },
    jobs=[job_ingestion, job_build_dbt_models],
    schedules=[schedule_ingestion_every_2d],
    sensors=[trigger_dbt_build_models_after_ingestion],
)
