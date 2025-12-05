from dagster import AssetExecutionContext, AssetKey, asset

from gws_pipeline.core import settings
from gws_pipeline.core.schemas.fetcher import Application
from gws_pipeline.core.process.with_polars import process_recent_activity


def make_processing_asset(app: Application):
    """Factory to create a processing asset per application."""

    @asset(
        name=f"{app.value.lower()}_process",
        group_name="Processing",
        deps=[AssetKey(f"{app.value.lower()}_raw_inc")],
        description=f"Processes {app.value} raw JSONL into Parquet events (and scopes for token).",
        kinds=["python", "polars"],
    )
    def _asset(context: AssetExecutionContext) -> None:
        events_count, scopes_count = process_recent_activity(app)
        base_out = settings.processed_data_dir / app.value.lower()
        context.log.info(
            f"[{app.value}] Processed"
            f" events={events_count}{f', scopes={scopes_count}' if app == Application.TOKEN else ''} into"
            f" {base_out.relative_to(settings.base_dir)}"
        )

    return _asset


processing_assets = [make_processing_asset(app) for app in Application]
