import gzip
import json
from datetime import datetime
from pathlib import Path

import pytest


def read_first_and_last_line(file_path: Path) -> tuple[str, str]:
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        first_line = next(f, None)
        last_line = first_line  # if only one line exists

        for line in f:
            last_line = line
        return first_line, last_line


@pytest.mark.parametrize(
    "date_str,hour",
    [
        ("2025-05-28", 23),
        ("2025-05-29", 1),
    ],
)
def test_first_and_last_event_within_hour_partition(date_str, hour):
    file_path = Path(f"data/raw/{date_str}/part_{hour:02}.jsonl.gz")
    assert file_path.exists(), f"File not found: {file_path}"

    start_time = datetime.fromisoformat(f"{date_str}T{hour:02}:00:00+00:00")
    end_time = datetime.fromisoformat(f"{date_str}T{hour:02}:59:59.999999+00:00")

    # Read first and last lines
    try:
        first_line, last_line = read_first_and_last_line(file_path)
        top_event = json.loads(first_line)
        bottom_event = json.loads(last_line)
    except Exception as e:
        pytest.fail(f"Failed to parse file {file_path}: {e}")

    for position, ev in [("first", top_event), ("last", bottom_event)]:
        event_time = datetime.fromisoformat(ev["id"]["time"].replace("Z", "+00:00"))
        assert (
            start_time <= event_time <= end_time
        ), f"{position.title()} event time {event_time} not in expected range: {start_time} to {end_time}"
