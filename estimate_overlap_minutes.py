import glob
import json
import math
import os
import re
from datetime import datetime, timezone, timedelta

import polars as pl

# Default filename pattern: epr_2025-05-30T15-00-00Z.jsonl.gz  -> end time = 2025-05-30 15:00:00Z
DEFAULT_FNAME_RE = re.compile(r"epr_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})Z\.jsonl\.gz")


def _percentile(values, p):
    if not values:
        return 0.0
    v = sorted(values)
    k = (len(v) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return v[int(k)]
    return v[f] + (k - f) * (v[c] - v[f])


def estimate_backward_overlap(
    per_run_glob: str = "data/runs/*.jsonl.gz",
    run_window_minutes: int = 60,
    filename_regex: re.Pattern = DEFAULT_FNAME_RE,
    percentile: int = 95,
    safety_pad_minutes: float = 1.0,
    tzinfo=timezone.utc,
):
    """
    Estimate a safe backward overlap (in minutes) for fetch jobs by measuring observed
    lateness of events across per-run JSONL snapshots.

    Returns a dict with stats, including the recommended overlap.
    """
    early_gaps = []
    late_gaps = []
    files = sorted(glob.glob(per_run_glob))
    runs_used = 0

    for fpath in files:
        m = filename_regex.search(os.path.basename(fpath))
        if not m:
            # Skip files that don't match the expected pattern
            continue

        end = datetime.strptime(m.group(1), "%Y-%m-%dT%H-%M-%S").replace(tzinfo=tzinfo)
        start = end - timedelta(minutes=run_window_minutes)

        try:
            sdf = pl.read_ndjson(fpath)
        except Exception:
            # Bad file, skip it
            continue

        if sdf.height == 0 or "event_time" not in sdf.columns:
            continue

        # Parse event_time as UTC datetime; drop rows that fail to parse
        sdf = sdf.with_columns(
            pl.col("event_time").str.strptime(pl.Datetime, fmt="%+", strict=False).alias("et")
        ).drop_nulls("et")

        if sdf.height == 0:
            continue

        mn = sdf["et"].min()
        mx = sdf["et"].max()

        # Always non-negative gaps:
        # how far back events arrived before the window start
        early_gap = max(0.0, (start - mn).total_seconds() / 60.0)
        # how far past events extended beyond the window end (rarely needed)
        late_gap = max(0.0, (mx - end).total_seconds() / 60.0)

        early_gaps.append(early_gap)
        late_gaps.append(late_gap)
        runs_used += 1

    p_early = _percentile(early_gaps, percentile)
    p_late = _percentile(late_gaps, percentile)
    recommended = int(math.ceil(p_early + safety_pad_minutes))

    result = {
        "files_considered": len(files),
        "runs_used": runs_used,
        "run_window_minutes": run_window_minutes,
        "percentile": percentile,
        "safety_pad_minutes": safety_pad_minutes,
        "p_early_gap_minutes": round(p_early, 3),
        "p_late_gap_minutes": round(p_late, 3),
        "mean_early_gap_minutes": round(sum(early_gaps) / runs_used, 3) if runs_used else 0.0,
        "max_early_gap_minutes": round(max(early_gaps), 3) if runs_used else 0.0,
        "recommended_backward_overlap_minutes": recommended,
    }
    return result


def estimate_backward_overlap_json(**kwargs) -> str:
    """
    Convenience wrapper that returns the same stats as JSON (compact, sorted keys).
    """
    stats = estimate_backward_overlap(**kwargs)
    return json.dumps(stats, separators=(",", ":"), sort_keys=True)


if __name__ == "__main__":
    # CLI-style usage: prints JSON to stdout
    print(estimate_backward_overlap_json())
