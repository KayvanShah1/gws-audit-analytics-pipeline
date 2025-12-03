from __future__ import annotations

import glob
import gzip
import json
import os
from datetime import timedelta
from pathlib import Path

import apache_beam as beam
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from apache_beam.options.pipeline_options import PipelineOptions
from gws_pipeline.core.config import get_logger, settings
from gws_pipeline.core.models import RawTokenActivity
from gws_pipeline.core.utils import fetch_last_run_timestamp, timed_run

logger = get_logger("TokenActivityProcessor")

# --------------------------------------------------------------------------- #
# Schemas                                                                     #
# --------------------------------------------------------------------------- #

EVENTS_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("unique_id", pa.string()),
        ("user", pa.string()),
        ("profile_id", pa.string()),
        ("ip", pa.string()),
        ("asn", pa.int64()),
        ("region_code", pa.string()),
        ("subdivision_code", pa.string()),
        ("event_type", pa.string()),
        ("event_name", pa.string()),
        ("method_name", pa.string()),
        ("num_bytes", pa.int64()),
        ("api_name", pa.string()),
        ("client_id", pa.string()),
        ("app_name", pa.string()),
        ("client_type", pa.string()),
        # aggregated scope info
        ("scope_count", pa.int32()),
        ("product_buckets", pa.list_(pa.string())),
        ("has_drive_scope", pa.bool_()),
        ("has_gmail_scope", pa.bool_()),
        ("has_admin_scope", pa.bool_()),
    ]
)

SCOPES_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("unique_id", pa.string()),
        ("scope_name", pa.string()),
        ("scope_family", pa.string()),
        ("product_bucket", pa.string()),
        ("service", pa.string()),
        ("is_readonly", pa.bool_()),
    ]
)


# --------------------------------------------------------------------------- #
# Beam DoFns                                                                  #
# --------------------------------------------------------------------------- #


class ReadCompressedJSONL(beam.DoFn):
    def process(self, file_path: str):
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                yield line


class ParseAndSplit(beam.DoFn):
    """
    Parse a raw JSON line into:
      - main output: event record (one per activity)
      - tagged 'scopes': scope records (0..N per activity)
      - tagged 'errors': parse/validation errors
    """

    def process(self, line):
        try:
            raw = json.loads(line)
            activity = RawTokenActivity(**raw)

            # main event record
            yield activity.to_event_record()

            # per-scope records
            for scope_rec in activity.iter_scope_records():
                yield beam.pvalue.TaggedOutput("scopes", scope_rec)

        except Exception as e:
            yield beam.pvalue.TaggedOutput("errors", {"error": str(e), "raw": line})


class WritePartitionedParquet(beam.DoFn):
    def __init__(self, output_dir: str, schema: pa.Schema):
        self.output_dir = output_dir
        self.schema = schema

    def process(self, element):
        date_key, records = element
        if not records:
            return

        output_path = Path(self.output_dir) / f"{date_key}.parquet"
        os.makedirs(output_path.parent, exist_ok=True)

        table = pa.Table.from_pylist(records, schema=self.schema)
        pq.write_table(table, output_path, compression="snappy")

        yield f"Wrote {len(records)} rows to {output_path.relative_to(settings.base_dir)}"


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def extract_partition_key(row, prefix: str):
    dt = row["timestamp"].strftime("%Y-%m-%d")
    return f"{prefix}_{dt}", row


def get_recent_files(base_dir: Path, hours: int = 48):
    last_run = fetch_last_run_timestamp()
    start_time = last_run - timedelta(hours=hours)

    matching_files: list[str] = []

    for i in range((last_run - start_time).days + 1):
        day = (start_time + timedelta(days=i)).strftime("%Y-%m-%d")
        day_dir = base_dir / day
        pattern = str(day_dir / "*.jsonl.gz")
        matching_files.extend(glob.glob(pattern))

    return matching_files


# --------------------------------------------------------------------------- #
# Beam pipeline                                                               #
# --------------------------------------------------------------------------- #


@timed_run
def process_recent_token_activity_beam():
    recent_files = get_recent_files(settings.raw_data_dir, hours=settings.DEFAULT_DELTA_HRS)
    if not recent_files:
        logger.warning("No files found for the last window.")
        return

    output_base = Path(settings.processed_data_dir)
    events_dir = str(output_base / "events")
    scopes_dir = str(output_base / "event_scopes")

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "Create file paths" >> beam.Create(recent_files)
            | "Read GZipped JSONL" >> beam.ParDo(ReadCompressedJSONL())
            | "Parse & Split" >> beam.ParDo(ParseAndSplit()).with_outputs("scopes", "errors", main="events")
        )

        # Events branch
        (
            parsed.events
            | "Events: key by unique_id" >> beam.Map(lambda x: (x["unique_id"], x))
            | "Events: dedup" >> beam.CombinePerKey(lambda vals: list(vals)[0])
            | "Events: values" >> beam.Values()
            | "Events: key by partition" >> beam.Map(extract_partition_key, "events")
            | "Events: group by partition" >> beam.GroupByKey()
            | "Events: write parquet"
            >> beam.ParDo(WritePartitionedParquet(output_dir=events_dir, schema=EVENTS_SCHEMA))
            | "Events: log output" >> beam.Map(lambda msg: logger.info(msg))
        )

        # Scopes branch (dedup per event + scope_name + bucket)
        (
            parsed.scopes
            | "Scopes: key for dedup"
            >> beam.Map(
                lambda r: (
                    (r["unique_id"], r["scope_name"], r.get("product_bucket")),
                    r,
                )
            )
            | "Scopes: dedup" >> beam.CombinePerKey(lambda vals: list(vals)[0])
            | "Scopes: values" >> beam.Values()
            | "Scopes: key by partition" >> beam.Map(extract_partition_key, "event_scopes")
            | "Scopes: group by partition" >> beam.GroupByKey()
            | "Scopes: write parquet"
            >> beam.ParDo(WritePartitionedParquet(output_dir=scopes_dir, schema=SCOPES_SCHEMA))
            | "Scopes: log output" >> beam.Map(lambda msg: logger.info(msg))
        )

        # Optional: log errors
        (parsed.errors | "Errors: log" >> beam.Map(lambda e: logger.error(f"Parse error: {e.get('error')}")))


# --------------------------------------------------------------------------- #
# Polars-based local pipeline                                                 #
# --------------------------------------------------------------------------- #


def _flatten_with_pydantic(files: list[str]) -> tuple[list[dict], list[dict]]:
    """
    Read JSONL.gz → Pydantic → lists of flat dicts.

    Returns:
      (event_records, scope_records)
    """
    event_records: list[dict] = []
    scope_records: list[dict] = []

    for fp in files:
        with gzip.open(fp, "rt", encoding="utf-8") as f:
            for line in f:

                raw = json.loads(line)
                activity = RawTokenActivity(**raw)

                # event
                try:
                    event_records.append(activity.to_event_record())
                except Exception as e:
                    logger.error(f"Failed to build event record in {fp}: {e}")
                    continue

                # scopes
                try:
                    for scope_rec in activity.iter_scope_records():
                        scope_records.append(scope_rec)
                except Exception as e:
                    logger.error(f"Failed to build scope records in {fp}: {e}")

    return event_records, scope_records


@timed_run
def process_recent_token_activity_polars_lazy():
    files = get_recent_files(settings.raw_data_dir, hours=settings.DEFAULT_DELTA_HRS)
    if not files:
        logger.warning("No recent raw files found.")
        return

    event_records, scope_records = _flatten_with_pydantic(files)

    if not event_records:
        logger.warning("No events parsed from recent raw files.")
        return

    # Build DataFrames
    events_df = pl.from_dicts(event_records)
    scopes_df = pl.from_dicts(scope_records) if scope_records else pl.DataFrame()

    # Events: dedup by unique_id and add date_key
    events_df = events_df.unique(subset=["unique_id"], keep="first").with_columns(
        pl.col("timestamp").dt.strftime("%Y-%m-%d").alias("date_key")
    )

    # Scopes: dedup per (unique_id, scope_name, product_bucket) and add date_key
    if scopes_df.height > 0:
        scopes_df = scopes_df.unique(subset=["unique_id", "scope_name", "product_bucket"], keep="first").with_columns(
            pl.col("timestamp").dt.strftime("%Y-%m-%d").alias("date_key")
        )

    output_base = Path(settings.processed_data_dir)
    events_dir = output_base / "events"
    scopes_dir = output_base / "event_scopes"
    events_dir.mkdir(parents=True, exist_ok=True)
    scopes_dir.mkdir(parents=True, exist_ok=True)

    event_col_order = EVENTS_SCHEMA.names
    scope_col_order = SCOPES_SCHEMA.names

    # Write events per day
    for date_key, sub in events_df.group_by("date_key", maintain_order=True):
        day = date_key[0]
        out_path = events_dir / f"events_{day}.parquet"
        table = sub.drop("date_key").select(event_col_order).to_arrow().cast(EVENTS_SCHEMA)
        pq.write_table(table, out_path, compression="snappy")
        logger.info(f"Wrote {sub.height} event rows to {out_path.relative_to(settings.base_dir)}")

    # Write scopes per day (if any)
    if scopes_df.height > 0:
        for date_key, sub in scopes_df.group_by("date_key", maintain_order=True):
            day = date_key[0]
            out_path = scopes_dir / f"event_scopes_{day}.parquet"
            table = sub.drop("date_key").select(scope_col_order).to_arrow().cast(SCOPES_SCHEMA)
            pq.write_table(table, out_path, compression="snappy")
            logger.info(f"Wrote {sub.height} scope rows to {out_path.relative_to(settings.base_dir)}")


if __name__ == "__main__":
    # Beam version (commented out if not needed locally)
    # process_recent_token_activity_beam()

    # Local Polars version
    process_recent_token_activity_polars_lazy()
