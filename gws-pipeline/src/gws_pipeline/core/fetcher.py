from dataclasses import dataclass
import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from gws_pipeline.core import get_logger, settings
from gws_pipeline.core.models import ActivityPathParams, ActivityQueryParams, Application, RunSnapshot
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = get_logger("Incremental Activity Fetcher")


# --- State handling ---
def load_last_run_timestamp(state_path: Path) -> datetime:
    """Load the last run timestamp from the given state file.
    If the file does not exist, return a fallback timestamp (now - DEFAULT_DELTA_HRS).
    Args:
        state_path (Path): Path to the state file
    Returns:
        datetime: Last run timestamp with overlap applied
    """
    # First run fallback to 48 hours ago
    if not state_path.exists():
        ts = datetime.now(timezone.utc) - timedelta(hours=settings.DEFAULT_DELTA_HRS)
        logger.warning(f"State file not found. Using fallback timestamp: {ts.isoformat()}")
        return ts

    with open(state_path, "r") as f:
        ts = datetime.fromisoformat(json.load(f)["last_run"]) - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)
        logger.info(f"Loaded last run timestamp: {ts.isoformat()}")
    return ts


def save_last_run_timestamp(state_path: Path, timestamp: datetime):
    """Persist the last run timestamp to the given state file.
    Args:
        state_path (Path): Path to the state file
        timestamp (datetime): Timestamp to save
    """
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w") as f:
        json.dump({"last_run": timestamp.isoformat()}, f)


def write_run_snapshot(snapshot: RunSnapshot, path: Path) -> None:
    """Persist the run snapshot to disk as pretty JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(snapshot.model_dump_json(indent=2))


# --- Partitioning ---
def get_partition_path(application: Application, event_time: datetime, window_idx: int) -> Path:
    """Returns a partitioned file path for the given event time.

    Args:
        application (Application): Application name for the activity
        event_time (datetime): Event timestamp
        window_idx (int): Fetch window index

    Returns:
        Path: Partitioned file path like 'YYYY-MM-DD/part_HH_wX.jsonl' or 'YYYY-MM-DD/part_wX.jsonl'
    """
    date_part = event_time.strftime("%Y-%m-%d")
    hour_part = event_time.strftime("%H")
    if application in (Application.TOKEN):
        return Path(date_part) / f"part_{hour_part}_w{window_idx}.jsonl"
    return Path(date_part) / f"part_w{window_idx}.jsonl"


def flush_buffer(buffer: List[dict], file_path: Path):
    """Flush the given buffer of events to the specified file path.
    Args:
        buffer (List[dict]): List of event dicts to write
        file_path (Path): Destination file path
    """
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


@dataclass
class Window:
    idx: int
    start: datetime
    end: datetime


def split_time_range(start: datetime, end: datetime, chunk_hours: int) -> List[Tuple[int, datetime, datetime]]:
    """Split [start, end] into contiguous windows of size chunk_hours.

    Args:
        start (datetime): Start of the time range
        end (datetime): End of the time range
        chunk_hours (int): Size of each chunk in hours

    Returns a list of (window_index, window_start, window_end) tuples.
    """
    min_span = timedelta(seconds=1)
    windows: List[Window] = []
    current = start
    widx = 0
    while current < end:
        next_chunk = min(current + timedelta(hours=chunk_hours), end)
        if (next_chunk - current) < min_span:
            break
        windows.append(Window(idx=widx, start=current, end=next_chunk))
        current = next_chunk
        widx += 1
    return windows


def window_hours_for(application: Application) -> int:
    """Determine the fetch window size in hours for the given application."""
    # High-volume apps: smaller windows (often many per run)
    if application in (Application.ADMIN):
        return 24
    # Low-volume apps: larger windows (often just 1–2 per run)
    if application in (Application.LOGIN, Application.SAML, Application.DRIVE):
        return 48
    # Default fallback
    return settings.WINDOW_HOURS


# --- Fetching token activity ---
def create_retry_session(creds: Credentials) -> AuthorizedSession:
    """Create an AuthorizedSession with retry strategy for transient errors."""
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


def build_endpoint(application: Application) -> str:
    """Build the full API endpoint URL for the given application."""
    params = ActivityPathParams.for_application(application)
    return params.get_full_endpoint(settings.base_url)


def fetch_window_to_files(
    session: AuthorizedSession,
    application: Application,
    start: datetime,
    end: datetime,
    raw_data_dir: Path,
    window_idx: int,
) -> Tuple[int, Optional[datetime], Optional[datetime]]:
    """
    Fetch token activity events for a single [start, end] window and write:

      • partitioned hourly JSONL(.gz) files under `raw_data_dir`
      • a per-run gzipped JSONL file under `per_run_dir`

    Args:
        session (AuthorizedSession): Authorized HTTP session
        application (Application): Application name for the activity
        start (datetime): Start of the fetch window
        end (datetime): End of the fetch window
        raw_data_dir (Path): Base directory for raw data files
        window_idx (int): Index of the fetch window

    Returns:
        Tuple[int, Optional[datetime], Optional[datetime]]: Number of events fetched,
        earliest event time, latest event time
    """
    endpoint = build_endpoint(application)

    partition_buffers: dict[Path, List[dict]] = defaultdict(list)

    next_page_token: Optional[str] = None
    num_events = 0
    earliest_event_time: Optional[datetime] = None
    latest_event_time: Optional[datetime] = None

    while True:
        query = ActivityQueryParams(startTime=start, endTime=end, pageToken=next_page_token)
        response = session.get(endpoint, params=query.to_dict())
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        if not items and not data.get("nextPageToken"):
            break

        for event in items:
            event_time = datetime.fromisoformat(event["id"]["time"])
            if earliest_event_time is None or event_time < earliest_event_time:
                earliest_event_time = event_time
            if latest_event_time is None or event_time > latest_event_time:
                latest_event_time = event_time

            partition_path = get_partition_path(application, event_time, window_idx)
            partition_buffers[partition_path].append(event)

            # flush partition buffer if large enough
            if len(partition_buffers[partition_path]) >= settings.BUFFER_SIZE:
                num_events += len(partition_buffers[partition_path])
                flush_buffer(partition_buffers[partition_path], raw_data_dir / partition_path)
                partition_buffers[partition_path].clear()

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    # final flush for partitioned files
    for partition_path, buffer in partition_buffers.items():
        if buffer:
            num_events += len(buffer)
            flush_buffer(buffer, raw_data_dir / partition_path)

    logger.info(
        f"[{application.value}] WINDOW [{start.isoformat()} -> {end.isoformat()}]. Fetched {num_events} events."
    )
    return num_events, earliest_event_time, latest_event_time
