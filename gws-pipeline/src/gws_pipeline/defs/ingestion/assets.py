from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from dagster import AssetExecutionContext, MetadataValue, asset
from gws_pipeline.core import settings
from gws_pipeline.core.fetcher import fetch_window_to_files, split_time_range, write_run_snapshot
from gws_pipeline.core.models import Application, RunSnapshot, WindowRange


def _run_raw_activity_incremental(context: AssetExecutionContext, application: Application) -> None:
    google_reports_api = context.resources.google_reports_api
    state_file = context.resources.state_file

    last_run = state_file.load_last_run(application)
    now = datetime.now(timezone.utc)

    windows = split_time_range(last_run, now, chunk_hours=settings.WINDOW_HOURS)
    if not windows:
        context.log.info(f"[{application.value}] No new time window to process.")
        return

    context.log.info(
        f"[{application.value}] Incremental run: {len(windows)} windows from "
        f"{last_run.isoformat()} to {now.isoformat()} "
        f"({settings.WINDOW_HOURS}-hour chunks)."
    )

    session = google_reports_api.get_session()
    earliest_window_start = windows[0][1]
    latest_event_time_global = earliest_event_time_global = None
    total_events = 0

    def process_window(window: Tuple[int, datetime, datetime]):
        w_idx, w_start, w_end = window
        context.log.info(f"[{application.value}] Fetching window {w_start.isoformat()} -> {w_end.isoformat()}")
        num_events, earliest_event_time, latest_event_time = fetch_window_to_files(
            session=session,
            application=application,
            start=w_start,
            end=w_end,
            raw_data_dir=settings.raw_data_dir / application.value.lower(),
            window_idx=w_idx,
        )
        return num_events, w_start, w_end, earliest_event_time, latest_event_time

    # 2) Run windows in parallel
    results: List[Tuple[int, datetime, datetime, Optional[datetime], Optional[datetime]]] = []
    with ThreadPoolExecutor(max_workers=settings.MAX_PARALLEL_WINDOWS) as pool:
        futures = [pool.submit(process_window, w) for w in windows]
        for fut in futures:
            num_events, w_start, w_end, earliest_event_time, latest_event_time = fut.result()

            if num_events == 0:
                continue
            total_events += num_events

            earliest_event_time_global = (
                earliest_event_time
                if earliest_event_time_global is None
                else min(earliest_event_time_global, earliest_event_time)
            )
            latest_event_time_global = (
                latest_event_time
                if latest_event_time_global is None
                else max(latest_event_time_global, latest_event_time)
            )
            results.append((num_events, w_start, w_end, earliest_event_time, latest_event_time))

    # Write per-run snapshot if enabled
    if settings.WRITE_SNAPSHOT:
        run_id = now.strftime("%Y%m%dT%H%M%S")
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
            windows=[WindowRange(start=res[1], end=res[2]) for res in results],
        )
        write_run_snapshot(snapshot, snapshot_path)

    # 3) Update cursor only after full success of this incremental run
    if latest_event_time_global is not None:
        state_file.save_last_run(latest_event_time_global, application)
        context.log.info(
            f"[{application.value}] Updated last_run to {latest_event_time_global.isoformat()} after successful"
            " incremental run."
        )

    # 4) Emit metadata for observability
    context.add_output_metadata(
        {
            "application": MetadataValue.text(application.value),
            "last_run_before": MetadataValue.text(last_run.isoformat()),
            "run_until": MetadataValue.text(now.isoformat()),
            "num_windows": MetadataValue.int(len(windows)),
            "window_hours": MetadataValue.int(settings.WINDOW_HOURS),
            "max_parallel_windows": MetadataValue.int(settings.MAX_PARALLEL_WINDOWS),
            "total_events": MetadataValue.int(total_events),
            "earliest_window_start": MetadataValue.text(earliest_window_start.isoformat()),
            "latest_event_time": MetadataValue.text(
                latest_event_time_global.isoformat() if latest_event_time_global else "None"
            ),
        }
    )

    context.log.info(f"[{application.value}] Incremental fetch completed: {total_events} events fetched in total.")


@asset(
    name="raw_token_activity_incremental",
    required_resource_keys={"google_reports_api", "state_file"},
    group_name="Ingestion",
    description="Incrementally fetches TOKEN activity and writes raw JSONL files.",
)
def raw_token_activity_incremental(context: AssetExecutionContext) -> None:
    _run_raw_activity_incremental(context, Application.TOKEN)


@asset(
    name="raw_login_activity_incremental",
    required_resource_keys={"google_reports_api", "state_file"},
    group_name="Ingestion",
    description="Incrementally fetches LOGIN activity and writes raw JSONL files.",
)
def raw_login_activity_incremental(context: AssetExecutionContext) -> None:
    _run_raw_activity_incremental(context, Application.LOGIN)


@asset(
    name="raw_saml_activity_incremental",
    required_resource_keys={"google_reports_api", "state_file"},
    group_name="Ingestion",
    description="Incrementally fetches SAML activity and writes raw JSONL files.",
)
def raw_saml_activity_incremental(context: AssetExecutionContext) -> None:
    _run_raw_activity_incremental(context, Application.SAML)


@asset(
    name="raw_admin_activity_incremental",
    required_resource_keys={"google_reports_api", "state_file"},
    group_name="Ingestion",
    description="Incrementally fetches ADMIN activity and writes raw JSONL files.",
)
def raw_admin_activity_incremental(context: AssetExecutionContext) -> None:
    _run_raw_activity_incremental(context, Application.ADMIN)


@asset(
    name="raw_drive_activity_incremental",
    required_resource_keys={"google_reports_api", "state_file"},
    group_name="Ingestion",
    description="Incrementally fetches DRIVE activity and writes raw JSONL files.",
)
def raw_drive_activity_incremental(context: AssetExecutionContext) -> None:
    _run_raw_activity_incremental(context, Application.DRIVE)
