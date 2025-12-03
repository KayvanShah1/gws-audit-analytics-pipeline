from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from gws_pipeline.core import get_logger, settings
from gws_pipeline.core.utils import fetch_last_run_timestamp, timed_run

logger = get_logger("TokenActivityAnalyzer")


def get_recent_files(data_dir: Path = settings.processed_data_dir) -> list[str]:
    recent_files = []
    cutoff_date = fetch_last_run_timestamp() - timedelta(hours=48)
    cutoff_date = cutoff_date.date()

    for file in data_dir.glob("events_*.parquet"):
        date_str = file.stem.replace("events_", "")
        file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        if file_date >= cutoff_date:
            recent_files.append(str(file))

    return recent_files


def prepare_dataframe(file_paths: list[str]) -> pl.DataFrame:
    if not file_paths:
        logger.warning("No recent files found.")
        return pl.DataFrame()
    df = pl.read_parquet(file_paths).sort("timestamp", descending=True)

    max_ts = df.select(pl.col("timestamp").max()).item()
    cutoff = max_ts - timedelta(hours=48)

    df = df.filter(pl.col("timestamp") >= cutoff)
    return df


def get_top_user(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    user_count = (
        df.group_by("user", "profile_id")
        .len(name="count")
        .sort("count", descending=True)
        .select(["user", "profile_id", "count"])
        .row(0, named=True)
    )
    return user_count


def get_top_method_by_bytes(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    method_bytes = (
        df.drop_nulls(subset=["method_name"])
        .group_by("method_name")
        .agg(pl.col("num_bytes").sum().alias("total_bytes"))
        .sort("total_bytes", descending=True)
        .select(["method_name", "total_bytes"])
        .row(0, named=True)
    )
    return method_bytes


@timed_run
def analyze():
    recent_files = get_recent_files()
    df = prepare_dataframe(recent_files)

    top_user = get_top_user(df)
    logger.info(f"Top user in last 48 hours: \n{top_user}")

    top_method = get_top_method_by_bytes(df)
    logger.info(f"Top API method by bytes: \n{top_method}")


if __name__ == "__main__":
    analyze()
    # Sample output:
    """
    User with most events:
    {'user': 'nancy.admin@hyenacapital.net', 'profile_id': '100230688039070881323', 'count': 401122}

    Top API by data returned:
    {'method_name': 'reports.activities.list', 'total_bytes': 38805605490}
    """
