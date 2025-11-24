from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import List, Tuple

from dagster import AssetExecutionContext, MetadataValue, asset
from gws_pipeline.core.config import settings
from gws_pipeline.core.fetcher import fetch_window_to_files, split_time_range


@asset(
    name="raw_token_activity_incremental",
    required_resource_keys={"google_reports_api", "state_file"},
    description=(
        "Incrementally fetches Google Workspace token activity from last_run -> now. "
        "Splits the range into time windows and fetches them in parallel, "
        "writing hourly-partitioned raw JSONL files and per-run logs."
    ),
    group_name="Ingestion",
)
def raw_token_activity_incremental(context: AssetExecutionContext) -> None:
    google_reports_api = context.resources.google_reports_api
    state_file = context.resources.state_file

    last_run = state_file.load_last_run()
    now = datetime.now(timezone.utc)

    windows = split_time_range(last_run, now, chunk_hours=settings.WINDOW_HOURS)
    if not windows:
        context.log.info("No new time window to process.")
        return

    context.log.info(
        f"Incremental run: {len(windows)} windows from "
        f"{last_run.isoformat()} to {now.isoformat()} "
        f"({settings.WINDOW_HOURS}-hour chunks)."
    )

    session = google_reports_api.get_session()

    earliest_window_start = windows[0][0]
    latest_event_time_global: datetime | None = last_run
    total_events = 0

    def process_window(window: Tuple[datetime, datetime]):
        w_start, w_end = window
        context.log.info(f"Fetching window {w_start.isoformat()} -> {w_end.isoformat()}")
        num_events, latest_event_time = fetch_window_to_files(
            session=session,
            start=w_start,
            end=w_end,
            run_start_time=now,
            raw_data_dir=settings.raw_data_dir,
            per_run_dir=settings.per_run_data_dir,
        )
        return num_events, w_start, w_end, latest_event_time

    # 2) Run windows in parallel
    results: List[Tuple[int, datetime, datetime, datetime | None]] = []
    with ThreadPoolExecutor(max_workers=settings.MAX_PARALLEL_WINDOWS) as pool:
        futures = [pool.submit(process_window, w) for w in windows]
        for fut in futures:
            num_events, w_start, w_end, latest_event_time = fut.result()
            total_events += num_events
            results.append((num_events, w_start, w_end, latest_event_time))

            if latest_event_time is not None:
                if latest_event_time_global is None or latest_event_time > latest_event_time_global:
                    latest_event_time_global = latest_event_time

    # 3) Update cursor only after full success of this incremental run
    if latest_event_time_global is not None:
        state_file.save_last_run(latest_event_time_global)
        context.log.info(
            f"Updated last_run to {latest_event_time_global.isoformat()} after successful incremental run."
        )

    # 4) Emit metadata for observability
    context.add_output_metadata(
        {
            "last_run_before": MetadataValue.text(last_run.isoformat()),
            "run_until": MetadataValue.text(now.isoformat()),
            "num_windows": MetadataValue.int(len(windows)),
            "window_hours": MetadataValue.int(settings.WINDOW_HOURS),
            "max_parallel_windows": MetadataValue.int(settings.MAX_PARALLEL_WINDOWS),
            "total_events": MetadataValue.int(total_events),
            "earliest_window_start": MetadataValue.text(earliest_window_start.isoformat()),
            "latest_event_time": MetadataValue.text(
                latest_event_time_global.isoformat() if latest_event_time_global else "none"
            ),
        }
    )

    context.log.info(f"Incremental fetch completed: {total_events} events fetched in total.")
