import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from .config import get_logger, settings
from .models import ActivityPathParams, ActivityQueryParams
from .utils import get_relative_path, timed_run

logger = get_logger("TokenActivityFetcher")


# --- State handling ---
def load_last_run_timestamp(state_path: Path) -> datetime:
    # First run fallback to 48 hours ago
    if not state_path.exists():
        ts = datetime.now(timezone.utc) - timedelta(hours=settings.DEFAULT_DELTA_HRS)
        logger.warning(f"State file not found. Using fallback timestamp: {ts.isoformat()}")
        return ts

    with open(state_path, "r") as f:
        ts = datetime.fromisoformat(json.load(f)["last_run"]) - timedelta(minutes=settings.OVERLAP_MINUTES)

    return ts


def save_last_run_timestamp(state_path: Path, timestamp: datetime):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w") as f:
        json.dump({"last_run": timestamp.isoformat()}, f)


# --- Partitioning ---
def get_partition_path(event_time: datetime) -> Path:
    date_part = event_time.strftime("%Y-%m-%d")
    hour_part = event_time.strftime("%H")
    return Path(date_part) / f"part_{hour_part}.jsonl"


def flush_buffer(buffer: List[dict], file_path: Path):
    if settings.USE_GZIP:
        file_path = file_path.with_suffix(file_path.suffix + ".gz")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(file_path, "at", encoding="utf-8", compresslevel=settings.GZIP_COMPRESSION_LVL) as f:
            for event in buffer:
                f.write(json.dumps(event) + "\n")
    else:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as f:
            for event in buffer:
                f.write(json.dumps(event) + "\n")
    # logger.info(f"Flushed {len(buffer)} events to {get_relative_path(file_path)}")


def split_time_range(start: datetime, end: datetime, chunk_hours: int) -> List[Tuple[datetime, datetime]]:
    """Split [start, end] into contiguous windows of size chunk_hours."""
    windows: List[Tuple[datetime, datetime]] = []
    current = start
    while current < end:
        next_chunk = min(current + timedelta(hours=chunk_hours), end)
        windows.append((current, next_chunk))
        current = next_chunk
    return windows


# --- Fetching token activity ---
def create_retry_session(creds: Credentials) -> AuthorizedSession:
    session = AuthorizedSession(creds)
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,  # exponential backoff: 1s, 2s, 4s...
        status_forcelist=[429, 500, 502, 503, 504],  # retry on these
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


@timed_run
def fetch_token_activity_buffered():
    run_start_time = datetime.now(timezone.utc)
    creds = Credentials.from_service_account_file(
        str(settings.creds_file),
        scopes=["https://www.googleapis.com/auth/admin.reports.audit.readonly"],
        subject=settings.subject,
    )
    session = create_retry_session(creds)

    last_run = load_last_run_timestamp(settings.state_file_fetcher)
    now = datetime.now(timezone.utc)

    logger.info(f"Fetching events from {last_run.isoformat()} to {now.isoformat()}...")

    next_token = None
    latest_event_time = last_run
    partition_buffers = defaultdict(list)

    path_params = ActivityPathParams()
    path = path_params.get_path()
    endpoint = f"{settings.base_url}{path}"
    logger.info(f"Using endpoint: {endpoint}")

    # Prepare per run file and buffer
    per_run_path = settings.per_run_data_dir / f"epr_{run_start_time.strftime('%Y-%m-%dT%H-%M-%SZ')}.jsonl.gz"
    per_run_path.parent.mkdir(parents=True, exist_ok=True)
    per_run_buffer = []
    num_events_fetched = 0

    with gzip.open(per_run_path, "wt", encoding="utf-8", compresslevel=settings.GZIP_COMPRESSION_LVL) as prf:
        while True:
            query = ActivityQueryParams(startTime=last_run, endTime=now, pageToken=next_token)

            response = session.get(endpoint, params=query.to_dict())
            response.raise_for_status()
            data = response.json()

            for event in data.get("items", []):
                event_time = datetime.fromisoformat(event["id"]["time"])
                latest_event_time = max(latest_event_time, event_time)
                partition_path = get_partition_path(event_time)
                partition_buffers[partition_path].append(event)

                # Add to per-run buffer
                per_run_buffer.append(event)

                # Check buffer sizes and flush if necessary
                if len(partition_buffers[partition_path]) >= settings.BUFFER_SIZE:
                    flush_buffer(
                        partition_buffers[partition_path], Path.joinpath(settings.raw_data_dir, partition_path)
                    )
                    partition_buffers[partition_path].clear()

                if len(per_run_buffer) >= settings.PER_RUN_BUFFER_SIZE:
                    prf.write(json.dumps(per_run_buffer) + "\n")
                    logger.info(f"Flushed {len(per_run_buffer)} events to {get_relative_path(per_run_path)}")
                    num_events_fetched += settings.PER_RUN_BUFFER_SIZE
                    per_run_buffer.clear()

            # Check if we have a next page token
            next_token = data.get("nextPageToken")
            if not next_token:
                break

            save_last_run_timestamp(settings.state_file_fetcher, latest_event_time)

    # Flush remaining buffers
    for partition_path, buffer in partition_buffers.items():
        if buffer:
            flush_buffer(buffer, Path.joinpath(settings.raw_data_dir, partition_path))

    if per_run_buffer:
        with gzip.open(per_run_path, "at", encoding="utf-8", compresslevel=settings.GZIP_COMPRESSION_LVL) as prf:
            for event in per_run_buffer:
                prf.write(json.dumps(event) + "\n")
        num_events_fetched += len(per_run_buffer)
        per_run_buffer.clear()

    save_last_run_timestamp(settings.state_file_fetcher, now)
    logger.info(f"Flushed events to {len(partition_buffers)} partitioned files.")
    logger.info(f"Fetched {num_events_fetched} events in total.")


def fetch_window_to_files(
    session: AuthorizedSession,
    start: datetime,
    end: datetime,
    run_start_time: datetime,
    raw_data_dir: Path,
    per_run_dir: Path,
) -> Tuple[int, Optional[datetime]]:
    """
    Fetch token activity events for a single [start, end] window and write:

      • partitioned hourly JSONL(.gz) files under `raw_data_dir`
      • a per-run gzipped JSONL file under `per_run_dir`

    Returns:
        (num_events, latest_event_time)
    """
    path_params = ActivityPathParams()
    path = path_params.get_path()
    endpoint = f"{settings.base_url}{path}"

    # Per-run folder (stable across all windows for the same run_start_time)
    run_id = run_start_time.strftime("%Y-%m-%dT%H-%M-%SZ")
    run_dir = per_run_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    window_id = f"{start.strftime('%Y%m%dT%H%M')}_{end.strftime('%Y%m%dT%H%M')}"
    per_run_name = f"window_{window_id}.jsonl.gz"
    per_run_path = run_dir / per_run_name

    partition_buffers: dict[Path, List[dict]] = defaultdict(list)
    per_run_buffer: List[dict] = []

    next_token: Optional[str] = None
    num_events = 0
    latest_event_time: Optional[datetime] = None

    def write_events(prf, events: List[dict]) -> None:
        nonlocal num_events
        for event in events:
            prf.write(json.dumps(event) + "\n")
        num_events += len(events)

    with gzip.open(per_run_path, "wt", encoding="utf-8", compresslevel=settings.GZIP_COMPRESSION_LVL) as prf:
        while True:
            query = ActivityQueryParams(startTime=start, endTime=end, pageToken=next_token)
            response = session.get(endpoint, params=query.to_dict())
            response.raise_for_status()
            data = response.json()

            items = data.get("items", [])
            if not items and not data.get("nextPageToken"):
                break

            for event in items:
                event_time = datetime.fromisoformat(event["id"]["time"])
                if latest_event_time is None or event_time > latest_event_time:
                    latest_event_time = event_time

                partition_path = get_partition_path(event_time)
                partition_buffers[partition_path].append(event)

                per_run_buffer.append(event)

                # flush partition buffer if large enough
                if len(partition_buffers[partition_path]) >= settings.BUFFER_SIZE:
                    flush_buffer(partition_buffers[partition_path], raw_data_dir / partition_path)
                    partition_buffers[partition_path].clear()

                # flush per-run buffer if large enough
                if len(per_run_buffer) >= settings.PER_RUN_BUFFER_SIZE:
                    write_events(prf, per_run_buffer)
                    per_run_buffer.clear()

            next_token = data.get("nextPageToken")
            if not next_token:
                break

        # final flush for partitioned files
        for partition_path, buffer in partition_buffers.items():
            if buffer:
                flush_buffer(buffer, raw_data_dir / partition_path)

        # final flush for per-run file
        if per_run_buffer:
            write_events(prf, per_run_buffer)
            per_run_buffer.clear()

    logger.info(f"WINDOW [{start.isoformat()} -> {end.isoformat()}]. Fetched {num_events} events.")

    return num_events, latest_event_time


if __name__ == "__main__":
    fetch_token_activity_buffered()
