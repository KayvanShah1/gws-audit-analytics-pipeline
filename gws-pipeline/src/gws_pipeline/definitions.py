from dagster import Definitions

from gws_pipeline.defs.ingestion.assets import raw_token_activity_incremental
from gws_pipeline.defs.transformation.beam.assets import processed_events
from gws_pipeline.defs.ingestion.resources import (
    GoogleReportsAPIResource,
    StateFileResource,
)
from gws_pipeline.jobs import job_all, schedule_hourly

defs = Definitions(
    assets=[raw_token_activity_incremental, processed_events],
    resources={
        "google_reports_api": GoogleReportsAPIResource(),
        "state_file": StateFileResource(),
    },
    jobs=[job_all],
    schedules=[schedule_hourly],
)

# from pathlib import Path

# from dagster import definitions, load_from_defs_folder


# @definitions
# def defs():
#     return load_from_defs_folder(path_within_project=Path(__file__).parent)
