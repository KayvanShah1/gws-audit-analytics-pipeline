from __future__ import annotations

import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import duckdb

from gws_pipeline.core import get_logger, settings
from gws_pipeline.core.schemas.fetcher import Application
from gws_pipeline.core.schemas.state import AppState
from gws_pipeline.core.state import load_state, update_duckdb_loader_state
from gws_pipeline.core.utils import timed_run

logger = get_logger("IncrementalLoader")


# ------------------------------------------------------------
# Helpers to discover processed parquet partitions
# ------------------------------------------------------------


def _iter_parquet_files(app: Application, kind: str) -> Iterable[Path]:
    """Yield processed parquet files for a given app and kind (e.g. 'events')."""
    root = settings.processed_data_dir / app.value.lower() / kind
    if not root.exists():
        logger.warning(f"[{app.value}] No processed {kind} directory at {root}")
        return []
    return sorted(root.glob(f"{kind}_*.parquet"))


def _earliest_partition(app: Application, kind: str) -> Optional[datetime]:
    """
    Find the earliest partition date based on file names like:
      events_YYYY-MM-DD.parquet
    """
    for fp in _iter_parquet_files(app, kind):
        try:
            dt = datetime.strptime(fp.stem.replace(f"{kind}_", ""), "%Y-%m-%d").replace(
                tzinfo=settings.DEFAULT_TIMEZONE
            )
            return dt
        except ValueError:
            continue
    return None


# ------------------------------------------------------------
# Cursor resolution for loader
# ------------------------------------------------------------


def _resolve_loader_start_from(app: Application, state: AppState) -> datetime:
    """
    Decide from which timestamp the loader should read processed data.

    Policy:
      - If loader has a watermark -> back up BACKWARD_OVERLAP_MINUTES from last_loaded_event_time.
      - Else, if there are processed partitions i.e. no data has been loaded yet -> start from earliest partition date.
      - Else -> fallback to a recent DEFAULT_DELTA_HRS window.
    """
    tz = settings.DEFAULT_TIMEZONE
    now = datetime.now(tz)

    # 1) Loader already ran: use its own cursor with a small overlap
    if state.duckdb_loader.last_loaded_event_time:
        start = (
            state.duckdb_loader.last_loaded_event_time - timedelta(minutes=settings.BACKWARD_OVERLAP_MINUTES)
        ).astimezone(tz)
        logger.info(
            f"[{app.value}] Loader cursor found; backing up "
            f"{settings.BACKWARD_OVERLAP_MINUTES} minutes to {start.isoformat()}"
        )
        return start

    # 2) First run: start from earliest processed partition
    earliest = _earliest_partition(app, "events")
    if earliest:
        logger.info(
            f"[{app.value}] No loader cursor; starting from earliest processed partition {earliest.isoformat()}"
        )
        return earliest

    # 3) Nothing processed yet: default to a recent window
    start = now - timedelta(hours=settings.DEFAULT_DELTA_HRS)
    logger.info(
        f"[{app.value}] No loader or processed state; defaulting to last "
        f"{settings.DEFAULT_DELTA_HRS}h window: {start.isoformat()}"
    )
    return start


# ------------------------------------------------------------
# Core incremental load logic (simplified)
# ------------------------------------------------------------


def _load_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    parquet_glob: str,
    start_from: datetime,
    unique_index: Optional[str] = None,  # e.g. "unique_id" or "unique_id, scope_name, product_bucket"
) -> Optional[datetime]:
    """
    Incrementally load data from parquet_glob into DuckDB table.

    Assumptions:
      - Processed parquet is already deduped on the logical key.
      - `timestamp` is the cursor.
      - We are okay re-reading an overlap window and replacing that tail.

    Strategy:
      - If no parquet files match -> skip.
      - Ensure schema and table exist, deriving schema from parquet (LIMIT 0).
      - Optionally create a UNIQUE index (for constraints / performance).
      - DELETE rows in {table} where timestamp > start_from.
      - INSERT rows from parquet where timestamp > start_from.
      - Return max(timestamp) from parquet for updated watermark.
    """
    # 0) Skip if no parquet files
    if not glob.glob(parquet_glob):
        logger.info(f"No parquet files found for glob: {parquet_glob}")
        return None

    # 1) Ensure schema and table exist with correct schema
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {settings.DUCKDB_LOADER_SCHEMA}")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} AS
        SELECT * FROM read_parquet('{parquet_glob}') WHERE 1=0
        """
    )

    # 2) Optional UNIQUE index (purely for constraint/perf)
    if unique_index:
        idx_name = f"idx_{table.replace('.', '_')}_uniq"
        try:
            con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON {table} ({unique_index})")
        except Exception as exc:
            logger.warning(f"Skipping unique index creation on {table} ({unique_index}): {exc}")

    rel_glob = Path(parquet_glob).relative_to(settings.base_dir)
    logger.info(f"Loading into {table} from glob={rel_glob}, start_from={start_from.isoformat()}")

    # 3) Delete overlapping tail from the target table
    try:
        con.execute(f"DELETE FROM {table} WHERE timestamp > ?", [start_from])
    except Exception as exc:
        logger.warning(f"Failed to delete overlap in {table}: {exc}")

    # 4) Insert new/overlapping rows from parquet
    con.execute(
        f"""
        INSERT INTO {table}
        SELECT *
        FROM read_parquet('{parquet_glob}')
        WHERE timestamp > ?
        """,
        [start_from],
    )

    # 5) Compute max timestamp from parquet for watermark
    max_ts = con.execute(
        f"""
        SELECT max(timestamp)
        FROM read_parquet('{parquet_glob}')
        WHERE timestamp > ?
        """,
        [start_from],
    ).fetchone()[0]

    return max_ts


# ------------------------------------------------------------
# Public API: load per-application
# ------------------------------------------------------------


@timed_run
def load_app(con: duckdb.DuckDBPyConnection, app: Application) -> Optional[datetime]:
    """
    Load a single application (ADMIN, LOGIN, DRIVE, TOKEN, SAML) into DuckDB.

    - Resolves loader start_from using loader state + earliest processed partition.
    - Loads events into <schema>.<app>_events.
    - For TOKEN, also loads event_scopes into <schema>.<app>_event_scopes.
    - Updates loader state with latest timestamp.
    """
    state_path = settings.state_dir / f"{app.value.lower()}.json"
    state = load_state(state_path)
    start_from = _resolve_loader_start_from(app, state)

    # Events
    events_glob = (settings.processed_data_dir / app.value.lower() / "events" / "events_*.parquet").as_posix()
    events_table = f"{settings.DUCKDB_LOADER_SCHEMA}.{app.value.lower()}_events"
    max_event_ts = _load_table(
        con,
        events_table,
        events_glob,
        start_from,
        unique_index="unique_id",
    )

    # Token scopes (only for TOKEN application)
    max_scope_ts = None
    if app == Application.TOKEN:
        scopes_glob = (
            settings.processed_data_dir / app.value.lower() / "event_scopes" / "event_scopes_*.parquet"
        ).as_posix()
        scopes_table = f"{settings.DUCKDB_LOADER_SCHEMA}.{app.value.lower()}_event_scopes"
        max_scope_ts = _load_table(
            con,
            scopes_table,
            scopes_glob,
            start_from,
            unique_index="unique_id, scope_name, product_bucket",
        )

    # Determine latest timestamp across all loaded slices
    latest_ts = (
        max(ts for ts in [max_event_ts, max_scope_ts] if ts is not None) if (max_event_ts or max_scope_ts) else None
    )

    logger.info(f"[{app.value}] Loaded events_ts={max_event_ts}, scopes_ts={max_scope_ts}, latest_ts={latest_ts}")

    # Update loader state if we loaded anything
    if latest_ts:
        update_duckdb_loader_state(
            state_path,
            latest_ts,
            run_id=datetime.now(settings.DEFAULT_TIMEZONE).strftime("%Y%m%dT%H%M%S"),
        )

    return latest_ts


# ------------------------------------------------------------
# CLI entrypoint
# ------------------------------------------------------------

if __name__ == "__main__":
    try:
        con = duckdb.connect(database=settings.duckdb_connection_string)
        # To load all applications:
        for app in Application:
            load_app(con, app)

        # Example: load only SAML
        # load_app(con, Application.SAML)
    finally:
        con.close()
