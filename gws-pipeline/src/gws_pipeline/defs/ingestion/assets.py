from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from dagster import AssetExecutionContext, AssetKey, MetadataValue, SourceAsset, asset
from gws_pipeline.core import settings
from gws_pipeline.core.db_loader import load_app
from gws_pipeline.core.fetcher import (
    fetch_window_to_files,
    split_time_range,
    window_hours_for,
    write_run_snapshot,
    Window,
)
from gws_pipeline.core.schemas.fetcher import Application, RunSnapshot, WindowRange

# --------------------------------------------------------------------------
# Incremental Assets for Fetching Raw Activity Data
# --------------------------------------------------------------------------


@dataclass
class WindowResult:
    window_idx: int
    start: datetime
    end: datetime
    num_events: int
    earliest_event_time: Optional[datetime]
    latest_event_time: Optional[datetime]


def _run_fetch_raw_activity_incremental(context: AssetExecutionContext, application: Application) -> None:
    """Runs an incremental fetch of raw activity for the given application. Chunks the time range since the last
    successful run into smaller windows, fetches each window in parallel, writes raw JSONL files, and updates the last
    run cursor upon successful completion. If enabled, writes a per-run snapshot file containing metadata about the run.
    Metadata about the run is also emitted for observability.

    Args:
        context (AssetExecutionContext): The Dagster asset execution context.
        application (Application): The application to fetch activity for.
    """
    google_reports_api = context.resources.google_reports_api
    state_file = context.resources.state_file

    # 0) Load last run cursor
    last_run = state_file.load_last_run(application)
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%S")
    snapshot_path = None

    # 1) Determine time windows to process
    chunk_hours = window_hours_for(application)
    windows = split_time_range(last_run, now, chunk_hours=chunk_hours)
    if not windows:
        context.log.info(f"[{application.value}] No new time window to process.")
        return

    context.log.info(
        f"[{application.value}] Incremental run: {len(windows)} windows from "
        f"{last_run.isoformat()} to {now.isoformat()} "
        f"({chunk_hours}-hour chunks)."
    )

    session = google_reports_api.get_session()
    earliest_window_start = windows[0].start
    latest_event_time_global = earliest_event_time_global = None
    total_events = 0

    def process_window(w: Window) -> WindowResult:
        context.log.info(f"[{application.value}] Fetching window {w.start.isoformat()} -> {w.end.isoformat()}")
        num_events, earliest_event_time, latest_event_time = fetch_window_to_files(
            session=session,
            application=application,
            start=w.start,
            end=w.end,
            raw_data_dir=settings.raw_data_dir / application.value.lower(),
            window_idx=w.idx,
        )
        return WindowResult(
            window_idx=w.idx,
            start=w.start,
            end=w.end,
            num_events=num_events,
            earliest_event_time=earliest_event_time,
            latest_event_time=latest_event_time,
        )

    # 2) Run windows in parallel
    results: List[WindowResult] = []
    with ThreadPoolExecutor(max_workers=settings.MAX_PARALLEL_WINDOWS) as pool:
        futures = [pool.submit(process_window, w) for w in windows]
        for fut in futures:
            res = fut.result()

            if res.num_events == 0:
                continue

            total_events += res.num_events

            if res.earliest_event_time is not None:
                earliest_event_time_global = (
                    res.earliest_event_time
                    if earliest_event_time_global is None
                    else min(earliest_event_time_global, res.earliest_event_time)
                )
            if res.latest_event_time is not None:
                latest_event_time_global = (
                    res.latest_event_time
                    if latest_event_time_global is None
                    else max(latest_event_time_global, res.latest_event_time)
                )

            results.append(res)

    # 3) Write per-run snapshot if enabled
    if settings.WRITE_SNAPSHOT:
        snapshot_path = settings.per_run_data_dir / application.value.lower() / f"snapshot_{run_id}.json"
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot = RunSnapshot(
            start=last_run,
            end=now,
            run_id=run_id,
            num_windows=len(windows),
            num_events=total_events,
            earliest_event_time=earliest_event_time_global,
            latest_event_time=latest_event_time_global,
            windows=[WindowRange(idx=res.window_idx, start=res.start, end=res.end) for res in results],
        )
        write_run_snapshot(snapshot, snapshot_path)

    # 4) Update cursor only after full success of this incremental run
    if latest_event_time_global is not None:
        state_file.save_last_run(latest_event_time_global, application, run_id=run_id, snapshot_path=snapshot_path)
        context.log.info(
            f"[{application.value}] Updated last_run to {latest_event_time_global.isoformat()} after successful"
            " incremental run."
        )

    # 5) Emit metadata for observability
    context.add_output_metadata(
        {
            "application": MetadataValue.text(application.value),
            "last_run_before": MetadataValue.text(last_run.isoformat()),
            "run_until": MetadataValue.text(now.isoformat()),
            "num_windows": MetadataValue.int(len(windows)),
            "window_hours": MetadataValue.int(chunk_hours),
            "max_parallel_windows": MetadataValue.int(settings.MAX_PARALLEL_WINDOWS),
            "total_events": MetadataValue.int(total_events),
            "earliest_window_start": MetadataValue.text(earliest_window_start.isoformat()),
            "latest_event_time": MetadataValue.text(
                latest_event_time_global.isoformat() if latest_event_time_global else "None"
            ),
        }
    )

    context.log.info(f"[{application.value}] Incremental fetch completed: {total_events} events fetched in total.")


def make_incremental_fetcher_asset(app: Application):
    @asset(
        name=f"{app.value.lower()}_raw_inc",
        required_resource_keys={"google_reports_api", "state_file"},
        group_name="raw_fetch",
        description=f"Incrementally fetches {app.value} activity and writes raw JSONL files.",
        kinds=["python"],
        deps=[AssetKey("google_reports_api_source")],
    )
    def _asset(context: AssetExecutionContext):
        _run_fetch_raw_activity_incremental(context, app)

    return _asset


fetcher_source_asset = SourceAsset(
    key=AssetKey("google_reports_api_source"),
    group_name="api",
    description="External Google Admin Reports API used to fetch audit activity.",
)


# --------------------------------------------------------------------------
# Incremental Assets for Loading Processed Activity Data into DuckDB
# --------------------------------------------------------------------------
def make_incremental_loader_asset(app: Application):
    @asset(
        name=f"{app.value.lower()}_load_inc",
        required_resource_keys={"duckdb_motherduck"},
        deps=[AssetKey(f"{app.value.lower()}_process")],
        group_name="warehouse_publish",
        description=f"Loads incrementally processed {app.value} activity data into DuckDB.",
        kinds=["python", "duckdb"],
    )
    def _asset(context: AssetExecutionContext):
        with context.resources.duckdb_motherduck.get_connection() as conn:
            latest = load_app(conn, app)
        context.log.info(f"[{app.value}] DuckDB load complete; latest ts={latest}")

    return _asset


# Create incremental assets for each application
incremental_fetcher_assets = [make_incremental_fetcher_asset(app) for app in Application]
incremental_loader_assets = [make_incremental_loader_asset(app) for app in Application]
