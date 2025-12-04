from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Tuple, Type

import pyarrow as pa
import pyarrow.parquet as pq
from gws_pipeline.core.config import get_logger, settings
from gws_pipeline.core.schemas.events import (
    BaseActivity,
    RawAdminActivity,
    RawDriveActivity,
    RawLoginActivity,
    RawSamlActivity,
    RawTokenActivity,
)
from gws_pipeline.core.schemas.fetcher import Application
from gws_pipeline.core.state import load_state, update_processor_state
from gws_pipeline.core.utils import timed_run

logger = get_logger("GenericActivityProcessor")

# Map application to the correct validator/formatter
MODEL_BY_APP: Dict[Application, Type[BaseActivity]] = {
    Application.TOKEN: RawTokenActivity,
    Application.ADMIN: RawAdminActivity,
    Application.LOGIN: RawLoginActivity,
    Application.DRIVE: RawDriveActivity,
    Application.SAML: RawSamlActivity,
}

BATCH_DAYS = 3  # process long gaps in 3-day batches


# --------------------------------------------------------------------------- #
# Helpers: file iteration / flattening                                        #
# --------------------------------------------------------------------------- #


def _iter_files_for_range(app: Application, start_date: date, end_date: date) -> List[str]:
    """
    Collect raw files for the given app between [start_date, end_date] (inclusive).
    Looks for both *.jsonl and *.jsonl.gz under app-specific day directories.
    """
    raw_root = settings.raw_data_dir / app.value.lower()
    if not raw_root.exists():
        return []

    files: List[str] = []
    for day_dir in raw_root.iterdir():
        if not day_dir.is_dir():
            continue
        try:
            day_dt = datetime.strptime(day_dir.name, "%Y-%m-%d").replace(tzinfo=settings.DEFAULT_TIMEZONE)
        except ValueError:
            continue
        day_date = day_dt.date()
        if start_date <= day_date <= end_date:
            files.extend(str(p) for p in day_dir.glob("*.jsonl"))
            files.extend(str(p) for p in day_dir.glob("*.jsonl.gz"))

    return sorted(set(files))


def _iter_lines(fp: str):
    """Yield lines from a possibly-gzipped JSONL file."""
    if fp.endswith(".gz"):
        with gzip.open(fp, "rt", encoding="utf-8") as f:
            for line in f:
                yield line
    else:
        with open(fp, "rt", encoding="utf-8") as f:
            for line in f:
                yield line


def _flatten(
    files: Iterable[str], model_cls: Type[BaseActivity], collect_scopes: bool
) -> Tuple[List[dict], List[dict]]:
    """
    Read raw JSONL(.gz) files → model_cls → flat dicts.

    Returns:
      (event_records, scope_records)
    """
    events: List[dict] = []
    scopes: List[dict] = []

    for fp in files:
        for line in _iter_lines(fp):
            raw = json.loads(line)
            activity = model_cls(**raw)
            events.append(activity.to_event_record())

            if collect_scopes and hasattr(activity, "iter_scope_records"):
                scopes.extend(list(activity.iter_scope_records()))

    return events, scopes


def _partition_by_date(records: Iterable[dict]) -> DefaultDict[str, List[dict]]:
    """
    Bucket records by 'YYYY-MM-DD' based on 'timestamp' field.
    """
    buckets: DefaultDict[str, List[dict]] = defaultdict(list)
    for rec in records:
        ts = rec.get("timestamp")
        if isinstance(ts, datetime):
            date_key = ts.strftime("%Y-%m-%d")
        else:
            # assume ISO-like string, take first 10 chars
            date_key = str(ts)[:10]
        buckets[date_key].append(rec)
    return buckets


def _earliest_partition_datetime(app: Application) -> Optional[datetime]:
    """Return the earliest partition date in raw data (used for first-run processing)."""
    raw_root = settings.raw_data_dir / app.value.lower()
    if not raw_root.exists():
        return None

    earliest: Optional[datetime] = None
    for day_dir in raw_root.iterdir():
        if not day_dir.is_dir():
            continue
        try:
            day_dt = datetime.strptime(day_dir.name, "%Y-%m-%d").replace(tzinfo=settings.DEFAULT_TIMEZONE)
        except ValueError:
            continue
        if earliest is None or day_dt < earliest:
            earliest = day_dt
    return earliest


def _write_parquet(partitions: Dict[str, List[dict]], out_dir: Path, prefix: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for date_key, rows in partitions.items():
        if not rows:
            continue
        table = pa.Table.from_pylist(rows)
        out_path = out_dir / f"{prefix}_{date_key}.parquet"
        pq.write_table(table, out_path, compression="snappy")
        logger.info(f"Wrote {len(rows)} rows to {out_path.relative_to(settings.base_dir)}")


# --------------------------------------------------------------------------- #
# Main processor                                                              #
# --------------------------------------------------------------------------- #


@timed_run
def process_recent_activity(app: Application, hours: int = settings.DEFAULT_DELTA_HRS) -> Tuple[int, int]:
    """Generic processor for any application using generic flatteners."""
    model_cls = MODEL_BY_APP[app]
    run_id = datetime.now(settings.DEFAULT_TIMEZONE).strftime("%Y%m%dT%H%M%S")

    # Load shared state for this application
    state_path = settings.state_dir / f"{app.value.lower()}.json"
    state = load_state(state_path)  # may be None depending on your state implementation

    earliest_raw_dt = _earliest_partition_datetime(app)

    # Determine starting point for processing (processor cursor -> earliest raw -> fetcher cursors -> fallback)
    start_from: Optional[datetime] = None

    if state and getattr(state, "processor", None) and state.processor.last_processed_event_time:
        # Start slightly before the last processed event (overlap)
        start_from = state.processor.last_processed_event_time - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)
    elif earliest_raw_dt:
        start_from = earliest_raw_dt
    elif state and getattr(state, "fetcher", None) and state.fetcher.last_event_time:
        start_from = state.fetcher.last_event_time - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)
    elif state and getattr(state, "fetcher", None) and state.fetcher.last_run:
        start_from = state.fetcher.last_run - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)

    if start_from is None:
        # Very first run or no usable state: use a recent window
        start_from = datetime.now(settings.DEFAULT_TIMEZONE) - timedelta(hours=hours)

    # Convert to date range; start from previous day of cursor
    overall_start_date = start_from.date() - timedelta(days=1)
    today_date = datetime.now(settings.DEFAULT_TIMEZONE).date()

    # Optional: cap by fetcher.last_event_time so we don't outrun ingestion
    if state and getattr(state, "fetcher", None) and state.fetcher.last_event_time:
        overall_end_date = min(today_date, state.fetcher.last_event_time.date())
    else:
        overall_end_date = today_date

    if overall_start_date > overall_end_date:
        logger.warning(f"[{app.value}] No date range to process ({overall_start_date} > {overall_end_date}).")
        return 0, 0

    logger.info(
        f"[{app.value}] Processing date range {overall_start_date} .. {overall_end_date} "
        f"(start_from={start_from.isoformat()})"
    )

    events_total = 0
    scopes_total = 0
    latest_ts: Optional[datetime] = None
    collect_scopes = app == Application.TOKEN

    # Process in 2-day batches
    current = overall_start_date
    while current <= overall_end_date:
        batch_end = min(current + timedelta(days=BATCH_DAYS - 1), overall_end_date)

        files = _iter_files_for_range(app, current, batch_end)
        if not files:
            logger.info(f"[{app.value}] No raw files found for {current}..{batch_end}.")
            current = batch_end + timedelta(days=1)
            continue

        events, scopes = _flatten(files, model_cls, collect_scopes)

        if not events:
            logger.warning(f"[{app.value}] No events parsed for {current}..{batch_end}.")
            current = batch_end + timedelta(days=1)
            continue

        # Track latest timestamp across all batches
        batch_latest = max(
            (rec.get("timestamp") for rec in events if rec.get("timestamp") is not None),
            default=None,
        )
        if batch_latest:
            latest_ts = batch_latest if latest_ts is None else max(latest_ts, batch_latest)

        # Partition and write daily Parquet partitions for events
        event_partitions = _partition_by_date(events)
        event_dir = settings.processed_data_dir / app.value.lower() / "events"
        _write_parquet(event_partitions, event_dir, "events")

        # Scopes, if applicable
        if collect_scopes and scopes:
            scope_partitions = _partition_by_date(scopes)
            scope_dir = settings.processed_data_dir / app.value.lower() / "event_scopes"
            _write_parquet(scope_partitions, scope_dir, "event_scopes")
            scopes_total += sum(len(v) for v in scope_partitions.values())

        events_total += len(events)

        logger.info(
            f"[{app.value}] Batch {current}..{batch_end}: {len(events)} events"
            f"{' and ' + str(scopes_total) + ' scopes' if collect_scopes else ''} "
            f"from {len(files)} files."
        )

        current = batch_end + timedelta(days=1)

    # Update processor cursor once per run
    if latest_ts:
        update_processor_state(state_path, latest_ts, run_id=run_id, status="success")
        logger.info(f"[{app.value}] Updated processor cursor to {latest_ts.isoformat()}")

    logger.info(
        f"[{app.value}] Finished processing: {events_total} events"
        f"{' and ' + str(scopes_total) + ' scopes' if collect_scopes else ''} in total."
    )

    return events_total, scopes_total


if __name__ == "__main__":
    for app in Application:
        process_recent_activity(app)
