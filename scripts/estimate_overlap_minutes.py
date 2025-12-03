import json
import math
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from rich.pretty import pretty_repr

from gws_pipeline.core import get_logger, settings
from gws_pipeline.core.utils import timed_run

logger = get_logger("EstimateOverlap")


def _percentile(values: list[float], p: float) -> float:
    """Compute the p-th percentile (0–100) with linear interpolation."""
    if not values:
        return 0.0
    v = sorted(values)
    k = (len(v) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return v[int(k)]
    return v[f] + (k - f) * (v[c] - v[f])


def _parse_utc(dt_str: str) -> datetime:
    """Parse ISO string and normalize to UTC."""
    dt = datetime.fromisoformat(dt_str)
    utc = ZoneInfo("UTC")
    if dt.tzinfo is None:
        # Stored as naive UTC representing UTC time
        return dt.replace(tzinfo=utc)
    # Convert any offset-aware datetime to UTC
    return dt.astimezone(utc)


@timed_run
def estimate_backward_overlap(
    run_window_minutes: Optional[int] = None,  # deprecated, kept for compatibility
    percentile: int = 95,
    safety_pad_minutes: float = 1.0,
    max_overlap_minutes: int = 10,
):
    """
    Estimate a safe backward overlap (in minutes) for fetch jobs by measuring, per run:

      head_gap = earliest_event_ts - start_ts
      tail_gap = end_ts - latest_event_ts

    where:
      - start_ts / end_ts are the requested window bounds,
      - earliest_event_ts / latest_event_ts are min/max event timestamps returned.

    For the Google token activity API, the primary signal is head_gap:
    "how many minutes from the requested start until the first event we actually see?"

    Returns a dict with:
      - recommended_backward_overlap_minutes
      - config
      - stats (runs + head/tail gap distributions)
    """
    if run_window_minutes is not None:
        logger.warning("'run_window_minutes' is deprecated for this script; window times are read from snapshots.")

    per_run_dir = Path(settings.per_run_data_dir)
    files = sorted(per_run_dir.glob("*.json"))

    head_gaps: list[float] = []
    tail_gaps: list[float] = []
    runs_used = 0

    for fpath in files:
        try:
            snapshot = json.loads(fpath.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Skipping malformed or unreadable file %s: %s", fpath, e)
            continue

        start_str = snapshot.get("start")
        end_str = snapshot.get("end")
        earliest_event_str = snapshot.get("earliest_event_time")
        latest_event_str = snapshot.get("latest_event_time")

        # Skip snapshots without full timing info
        if not all([start_str, end_str, earliest_event_str, latest_event_str]):
            continue

        start_ts = _parse_utc(start_str)
        end_ts = _parse_utc(end_str)
        earliest_event_ts = _parse_utc(earliest_event_str)
        latest_event_ts = _parse_utc(latest_event_str)

        # Head gap: how many minutes after the requested start the first event appears.
        # If earliest_event_ts <= start_ts, we treat head gap as 0 (no missing head).
        head_gap = max(0.0, (earliest_event_ts - start_ts).total_seconds() / 60.0)

        # Tail gap: how many minutes before the requested end the last event appears.
        # If latest_event_ts >= end_ts, we treat tail gap as 0 (no missing tail).
        tail_gap = max(0.0, (end_ts - latest_event_ts).total_seconds() / 60.0)

        head_gaps.append(head_gap)
        tail_gaps.append(tail_gap)
        runs_used += 1

    # No usable runs -> return minimal metadata
    if runs_used == 0:
        return {
            "recommended_backward_overlap_minutes": 0,
            "config": {
                "percentile": percentile,
                "safety_pad_minutes": safety_pad_minutes,
                "max_overlap_minutes": max_overlap_minutes,
                "snapshots_dir": str(per_run_dir.relative_to(settings.base_dir)),
            },
            "stats": {
                "runs": {
                    "files_considered": len(files),
                    "runs_used": 0,
                },
                "head_gap_minutes": None,
                "tail_gap_minutes": None,
            },
        }

    # Percentiles
    p_head = _percentile(head_gaps, percentile)
    p_tail = _percentile(tail_gaps, percentile)

    # Overlap is driven only by the head gap: how far the data lags from the window start.
    recommended = int(math.ceil(p_head + safety_pad_minutes))
    if max_overlap_minutes is not None:
        recommended = min(recommended, max_overlap_minutes)

    result = {
        "recommended_backward_overlap_minutes": recommended,
        "config": {
            "percentile": percentile,
            "safety_pad_minutes": safety_pad_minutes,
            "max_overlap_minutes": max_overlap_minutes,
            "snapshots_dir": str(per_run_dir.relative_to(settings.base_dir)),
        },
        "stats": {
            "runs": {
                "files_considered": len(files),
                "runs_used": runs_used,
            },
            "head_gap_minutes": {
                "p": round(p_head, 3),
                "mean": round(sum(head_gaps) / runs_used, 3),
                "max": round(max(head_gaps), 3),
            },
            "tail_gap_minutes": {
                "p": round(p_tail, 3),
                "mean": round(sum(tail_gaps) / runs_used, 3),
                "max": round(max(tail_gaps), 3),
            },
        },
    }
    return result


if __name__ == "__main__":
    logger.info(pretty_repr(estimate_backward_overlap()))
