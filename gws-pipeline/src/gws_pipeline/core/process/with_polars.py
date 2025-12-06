from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Tuple, Type

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from gws_pipeline.core.config import get_logger, settings
from gws_pipeline.core.schemas.parquet_schemas import EVENT_SCHEMA_BY_APP, SCOPE_SCHEMA_BY_APP
from gws_pipeline.core.schemas.events import (
    BaseActivity,
    RawAdminActivity,
    RawDriveActivity,
    RawLoginActivity,
    RawSamlActivity,
    RawTokenActivity,
)
from gws_pipeline.core.schemas.fetcher import Application
from gws_pipeline.core.schemas.state import AppState
from gws_pipeline.core.state import load_state, update_processor_state
from gws_pipeline.core.utils import timed_run

logger = get_logger("ActivityProcessor")

# Map application to the correct validator/formatter
MODEL_BY_APP: Dict[Application, Type[BaseActivity]] = {
    Application.TOKEN: RawTokenActivity,
    Application.ADMIN: RawAdminActivity,
    Application.LOGIN: RawLoginActivity,
    Application.DRIVE: RawDriveActivity,
    Application.SAML: RawSamlActivity,
}


# --------------------------------------------------------------------------- #
# Low-level helpers                                                           #
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


def _iter_lines(fp: str) -> Iterable[str]:
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
    files: Iterable[str],
    model_cls: Type[BaseActivity],
    collect_scopes: bool,
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


def _dedup_records(records: List[dict], subset: List[str]) -> List[dict]:
    """
    Deduplicate records using Polars, keeping the latest by timestamp if present.
    """
    if not records:
        return []

    table = pa.Table.from_pylist(records)
    df = pl.from_arrow(table)

    # Sort to ensure keep="last" keeps the most recent timestamp per key
    if "timestamp" in df.columns:
        df = df.sort("timestamp")

    deduped = df.unique(subset=subset, keep="last")
    return deduped.to_dicts()


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


def _write_parquet(
    partitions: Dict[str, List[dict]], out_dir: Path, prefix: str, schema: pa.Schema | None = None
) -> None:
    """Write partitioned records to Parquet files under out_dir with given prefix."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for date_key, rows in partitions.items():
        if not rows:
            continue
        # Validate against schema if provided
        table = pa.Table.from_pylist(rows, schema=schema) if schema else pa.Table.from_pylist(rows)
        out_path = out_dir / f"{prefix}_{date_key}.parquet"
        pq.write_table(table, out_path, compression="snappy")
        logger.info(f"Wrote {len(rows)} rows to {out_path.relative_to(settings.base_dir)}")


# --------------------------------------------------------------------------- #
# Mid-level helpers: dates, batches, writing                                  #
# --------------------------------------------------------------------------- #


def _make_run_id() -> str:
    """Generate a run ID based on current timestamp."""
    return datetime.now(settings.DEFAULT_TIMEZONE).strftime("%Y%m%dT%H%M%S")


def _resolve_start_from(app: Application, state: AppState, hours: int) -> datetime:
    """Pick the cursor datetime we should start from."""
    tz = settings.DEFAULT_TIMEZONE
    now = datetime.now(tz)

    # 1) Processor cursor (with overlap)
    if state and getattr(state, "processor", None) and state.processor.last_processed_event_time:
        return state.processor.last_processed_event_time - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)

    # 2) Earliest raw partition
    earliest_raw_dt = _earliest_partition_datetime(app)
    if earliest_raw_dt:
        return earliest_raw_dt

    # 3) Fetcher state if available
    if state and getattr(state, "fetcher", None):
        if state.fetcher.last_event_time:
            return state.fetcher.last_event_time - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)
        if state.fetcher.last_run:
            return state.fetcher.last_run - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)

    # 4) First run / no state: recent window
    return now - timedelta(hours=hours)


def _compute_date_window(app: Application, state, start_from: datetime) -> Tuple[date, date]:
    """Return (start_date, end_date) for raw partitions."""
    tz = settings.DEFAULT_TIMEZONE
    today = datetime.now(tz).date()

    # Start one day before cursor to be safe around midnight boundaries
    start_date = start_from.date() - timedelta(days=1)

    if state and getattr(state, "fetcher", None) and state.fetcher.last_event_time:
        end_date = min(today, state.fetcher.last_event_time.date())
    else:
        end_date = today

    return start_date, end_date


def _iter_date_batches(start: date, end: date, batch_days: int) -> Iterable[Tuple[date, date]]:
    """Yield (batch_start, batch_end) windows covering [start, end]."""
    current = start
    while current <= end:
        batch_end = min(current + timedelta(days=batch_days - 1), end)
        yield current, batch_end
        current = batch_end + timedelta(days=1)


def _write_event_partitions(app: Application, events: List[dict]) -> None:
    """Write event records to Parquet partitions."""
    partitions = _partition_by_date(events)
    event_dir = settings.processed_data_dir / app.value.lower() / "events"
    schema = EVENT_SCHEMA_BY_APP.get(app)
    _write_parquet(partitions, event_dir, "events", schema=schema)


def _write_scope_partitions(app: Application, scopes: List[dict]) -> int:
    """Write scope records to Parquet partitions. Returns number of scopes written."""
    partitions = _partition_by_date(scopes)
    scope_dir = settings.processed_data_dir / app.value.lower() / "event_scopes"
    schema = SCOPE_SCHEMA_BY_APP.get(app)
    _write_parquet(partitions, scope_dir, "event_scopes", schema=schema)
    return sum(len(v) for v in partitions.values())


def _run_batches(
    app: Application,
    model_cls: Type[BaseActivity],
    start_date: date,
    end_date: date,
    collect_scopes: bool,
) -> Tuple[int, int, Optional[datetime]]:
    """Run all date batches and return (events_total, scopes_total, latest_ts)."""
    events_total = 0
    scopes_total = 0
    latest_ts: Optional[datetime] = None

    for batch_start, batch_end in _iter_date_batches(start_date, end_date, batch_days=settings.PROCESS_BATCH_DAYS):
        files = _iter_files_for_range(app, batch_start, batch_end)
        if not files:
            logger.info(f"[{app.value}] No raw files found for {batch_start}..{batch_end}.")
            continue

        events, scopes = _flatten(files, model_cls, collect_scopes)

        # Deduplicate events by unique_id, keeping the latest timestamp
        before_events = len(events)
        events = _dedup_records(events, subset=["unique_id"])
        if before_events != len(events):
            logger.info(f"[{app.value}] Deduped events: {before_events} -> {len(events)}")

        # Deduplicate scopes by (unique_id, scope_name, product_bucket)
        if collect_scopes and scopes:
            before_scopes = len(scopes)
            scopes = _dedup_records(scopes, subset=["unique_id", "scope_name", "product_bucket"])
            if before_scopes != len(scopes):
                logger.info(f"[{app.value}] Deduped scopes: {before_scopes} -> {len(scopes)}")

        if not events:
            logger.warning(f"[{app.value}] No events parsed for {batch_start}..{batch_end}.")
            continue

        # Latest timestamp for this batch
        batch_latest = max(
            (rec.get("timestamp") for rec in events if rec.get("timestamp") is not None),
            default=None,
        )
        if batch_latest:
            latest_ts = batch_latest if latest_ts is None else max(latest_ts, batch_latest)

        _write_event_partitions(app, events)

        if collect_scopes and scopes:
            scopes_written = _write_scope_partitions(app, scopes)
            scopes_total += scopes_written

        events_total += len(events)

        logger.info(
            f"[{app.value}] Batch {batch_start}..{batch_end}: {len(events)} events"
            f"{' and ' + str(scopes_total) + ' scopes' if collect_scopes else ''} "
            f"from {len(files)} files."
        )

    return events_total, scopes_total, latest_ts


# --------------------------------------------------------------------------- #
# Main processor                                                              #
# --------------------------------------------------------------------------- #


@timed_run
def process_recent_activity(app: Application, hours: int = settings.DEFAULT_DELTA_HRS) -> Tuple[int, int]:
    """Generic processor for any application using generic flatteners."""
    model_cls = MODEL_BY_APP[app]
    run_id = _make_run_id()

    # Load shared state for this application
    state_path = settings.state_dir / f"{app.value.lower()}.json"
    state = load_state(state_path)  # may be None depending on your state implementation

    start_from = _resolve_start_from(app, state, hours)
    start_date, end_date = _compute_date_window(app, state, start_from)

    if start_date > end_date:
        logger.warning(f"[{app.value}] No date range to process ({start_date} > {end_date}).")
        return 0, 0

    logger.info(f"[{app.value}] Processing date range {start_date} .. {end_date} (start_from={start_from.isoformat()})")

    collect_scopes = app == Application.TOKEN

    events_total, scopes_total, latest_ts = _run_batches(
        app=app,
        model_cls=model_cls,
        start_date=start_date,
        end_date=end_date,
        collect_scopes=collect_scopes,
    )

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
