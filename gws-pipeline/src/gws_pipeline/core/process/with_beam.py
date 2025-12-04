from __future__ import annotations

import glob
import gzip
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Type

import apache_beam as beam
import pyarrow as pa
import pyarrow.parquet as pq
from apache_beam.options.pipeline_options import PipelineOptions
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
from gws_pipeline.core.state import load_state

logger = get_logger("BeamActivityProcessor")

# Map application to the correct validator/formatter
MODEL_BY_APP: Dict[Application, Type[BaseActivity]] = {
    Application.TOKEN: RawTokenActivity,
    Application.ADMIN: RawAdminActivity,
    Application.LOGIN: RawLoginActivity,
    Application.DRIVE: RawDriveActivity,
    Application.SAML: RawSamlActivity,
}

# Token-specific scope schema
TOKEN_SCOPES_SCHEMA = pa.schema(
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


class ReadJSONL(beam.DoFn):
    def process(self, file_path: str):
        opener = gzip.open if file_path.endswith(".gz") else open
        with opener(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                yield line


class ParseAndSplit(beam.DoFn):
    """
    Parse a raw JSON line into:
      - main output: event record (one per activity)
      - tagged 'scopes': scope records (0..N per activity) if enabled
      - tagged 'errors': parse/validation errors
    """

    def __init__(self, model_cls: Type[BaseActivity], collect_scopes: bool):
        self.model_cls = model_cls
        self.collect_scopes = collect_scopes

    def process(self, line: str):
        try:
            raw = json.loads(line)
            activity = self.model_cls(**raw)

            # main event record
            yield activity.to_event_record()

            # per-scope records (token only)
            if self.collect_scopes and hasattr(activity, "iter_scope_records"):
                for scope_rec in activity.iter_scope_records():
                    yield beam.pvalue.TaggedOutput("scopes", scope_rec)

        except Exception as e:
            yield beam.pvalue.TaggedOutput("errors", {"error": str(e), "raw": line})


class WritePartitionedParquet(beam.DoFn):
    def __init__(self, output_dir: str, schema: Optional[pa.Schema] = None):
        self.output_dir = output_dir
        self.schema = schema

    def process(self, element):
        date_key, records = element
        if not records:
            return

        output_path = Path(self.output_dir) / f"{date_key}.parquet"
        os.makedirs(output_path.parent, exist_ok=True)

        table = pa.Table.from_pylist(records, schema=self.schema) if self.schema else pa.Table.from_pylist(records)
        pq.write_table(table, output_path, compression="snappy")

        yield f"Wrote {len(records)} rows to {output_path.relative_to(settings.base_dir)}"


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _extract_partition_key(row, prefix: str):
    dt = row["timestamp"].strftime("%Y-%m-%d")
    return f"{prefix}_{dt}", row


def _get_recent_files(base_dir: Path, hours: int, application: Application) -> list[str]:
    state_path = settings.state_dir / f"{application.value.lower()}.json"
    state = load_state(state_path)
    last_run = state.fetcher.last_run
    start_time = last_run - timedelta(hours=hours)

    matching_files: list[str] = []

    for i in range((last_run - start_time).days + 1):
        day = (start_time + timedelta(days=i)).strftime("%Y-%m-%d")
        day_dir = base_dir / application.value.lower() / day
        matching_files.extend(glob.glob(str(day_dir / "*.jsonl")))
        matching_files.extend(glob.glob(str(day_dir / "*.jsonl.gz")))

    return matching_files


def _build_pipeline(
    pipeline: beam.Pipeline,
    recent_files: List[str],
    model_cls: Type[BaseActivity],
    collect_scopes: bool,
    events_dir: str,
    scopes_dir: Optional[str],
) -> None:
    parsed = (
        pipeline
        | "Create file paths" >> beam.Create(recent_files)
        | "Read JSONL" >> beam.ParDo(ReadJSONL())
        | "Parse & Split"
        >> beam.ParDo(ParseAndSplit(model_cls=model_cls, collect_scopes=collect_scopes)).with_outputs(
            "scopes", "errors", main="events"
        )
    )

    # Events branch
    (
        parsed.events
        | "Events: key by unique_id" >> beam.Map(lambda x: (x["unique_id"], x))
        | "Events: dedup" >> beam.CombinePerKey(lambda vals: list(vals)[0])
        | "Events: values" >> beam.Values()
        | "Events: key by partition" >> beam.Map(_extract_partition_key, "events")
        | "Events: group by partition" >> beam.GroupByKey()
        | "Events: write parquet" >> beam.ParDo(WritePartitionedParquet(output_dir=events_dir, schema=None))
        | "Events: log output" >> beam.Map(lambda msg: logger.info(msg))
    )

    if collect_scopes and scopes_dir:
        (
            parsed.scopes
            | "Scopes: key for dedup"
            >> beam.Map(
                lambda r: (
                    (r["unique_id"], r.get("scope_name"), r.get("product_bucket")),
                    r,
                )
            )
            | "Scopes: dedup" >> beam.CombinePerKey(lambda vals: list(vals)[0])
            | "Scopes: values" >> beam.Values()
            | "Scopes: key by partition" >> beam.Map(_extract_partition_key, "event_scopes")
            | "Scopes: group by partition" >> beam.GroupByKey()
            | "Scopes: write parquet"
            >> beam.ParDo(WritePartitionedParquet(output_dir=scopes_dir, schema=TOKEN_SCOPES_SCHEMA))
            | "Scopes: log output" >> beam.Map(lambda msg: logger.info(msg))
        )

    # Optional: log errors
    (parsed.errors | "Errors: log" >> beam.Map(lambda e: logger.error(f"Parse error: {e.get('error')}")))


# --------------------------------------------------------------------------- #
# Beam pipeline entrypoints                                                   #
# --------------------------------------------------------------------------- #


def process_recent_activity_beam(application: Application):
    model_cls = MODEL_BY_APP[application]
    collect_scopes = application == Application.TOKEN

    recent_files = _get_recent_files(settings.raw_data_dir, hours=settings.DEFAULT_DELTA_HRS, application=application)
    if not recent_files:
        logger.warning(f"[{application.value}] No files found for the last window.")
        return

    output_base = Path(settings.processed_data_dir) / application.value.lower()
    events_dir = str(output_base / "events")
    scopes_dir = str(output_base / "event_scopes") if collect_scopes else None

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        _build_pipeline(
            pipeline=p,
            recent_files=recent_files,
            model_cls=model_cls,
            collect_scopes=collect_scopes,
            events_dir=events_dir,
            scopes_dir=scopes_dir,
        )


def process_recent_token_activity_beam():
    process_recent_activity_beam(Application.TOKEN)


def process_recent_login_activity_beam():
    process_recent_activity_beam(Application.LOGIN)


def process_recent_saml_activity_beam():
    process_recent_activity_beam(Application.SAML)


def process_recent_admin_activity_beam():
    process_recent_activity_beam(Application.ADMIN)


def process_recent_drive_activity_beam():
    process_recent_activity_beam(Application.DRIVE)


if __name__ == "__main__":
    for app in Application:
        process_recent_activity_beam(app)
