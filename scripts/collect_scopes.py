#!/usr/bin/env python
"""
Scan raw Google Workspace token activity logs and compile a list of scopes.

Usage:
    python collect_scopes.py /path/to/raw_dir [--out scopes.csv]

The script:
  - Reads *.jsonl / *.json / *.gz files (NDJSON-like)
  - Handles both top-level {"items": [...]} and per-line activity objects
  - Aggregates unique (scope_name, product_bucket, service, is_readonly) combos
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Models for raw token activity                                              #
# --------------------------------------------------------------------------- #


class EventParameter(BaseModel):
    """Single parameter inside an event."""

    name: str
    value: Optional[str] = None

    int_value: Optional[int] = Field(default=None, alias="intValue")
    multi_value: Optional[List[str]] = Field(default=None, alias="multiValue")
    multi_message_value: Optional[List[Dict[str, Any]]] = Field(default=None, alias="multiMessageValue")

    class Config:
        allow_population_by_field_name = True


class Event(BaseModel):
    """Single event within a token activity."""

    name: str
    type: str
    parameters: List[EventParameter]


class NetworkInfo(BaseModel):
    """Network information associated with a token activity event."""

    ip_asn: Optional[List[int]] = Field(default=None, alias="ipAsn")
    region_code: Optional[str] = Field(default=None, alias="regionCode")
    subdivision_code: Optional[str] = Field(default=None, alias="subdivisionCode")

    class Config:
        allow_population_by_field_name = True


class RawTokenActivity(BaseModel):
    """
    Raw token activity event as returned by the Google Workspace Reports API.

    We only use a subset of fields and expose iter_scope_records() to
    enumerate scope-related rows.
    """

    id: Dict[str, Any]
    actor: Dict[str, Any]
    events: List[Event]

    ip_address: Optional[str] = Field(default=None, alias="ipAddress")
    network_info: Optional[NetworkInfo] = Field(default=None, alias="networkInfo")

    class Config:
        allow_population_by_field_name = True

    # Mapping from scope "family" → coarse service label
    SERVICE_FAMILY_MAP: ClassVar[Dict[str, str]] = {
        "admin": "admin",
        "apps": "admin",
        "cloud-identity": "admin",
        "cloudplatformprojects": "cloud_platform",
        "drive": "drive",
        "gmail": "gmail",
        "script": "apps_script",
    }

    @staticmethod
    def _normalize_non_google_scope(scope_name: str) -> str:
        """
        Optional: handle generic OAuth scopes that aren't under
        https://www.googleapis.com/auth/.
        """
        simple = (scope_name or "").lower()

        if simple in {"openid", "email", "profile"}:
            return "identity"

        # Could be a third-party or other provider's scope
        return "other"

    @classmethod
    def _derive_service_from_scope(cls, scope_name: str) -> str:
        if not scope_name:
            return "other"

        prefix = "https://www.googleapis.com/auth/"
        if not scope_name.startswith(prefix):
            return cls._normalize_non_google_scope(scope_name)

        tail = scope_name[len(prefix) :]
        first_segment = tail.split("/", 1)[0]
        core = first_segment.split(".", 1)[0]

        return cls.SERVICE_FAMILY_MAP.get(core, "other")

    def _parse_timestamp(self) -> datetime:
        ts_raw = self.id.get("time")
        # Example: "2025-11-27T03:11:11.616Z"
        return datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))

    def _first_event(self) -> Optional[Event]:
        return self.events[0] if self.events else None

    def _iter_scope_data_messages(self, event: Event) -> Iterable[Dict[str, Any]]:
        """Yield raw messages from the 'scope_data' parameter, if present."""
        for param in event.parameters:
            if param.name == "scope_data" and param.multi_message_value:
                for msg in param.multi_message_value:
                    yield msg

    def _iter_scope_list_values(self, event: Event) -> Iterable[str]:
        """Yield scope strings from the 'scope' parameter, if present."""
        for param in event.parameters:
            if param.name == "scope" and param.multi_value:
                for scope_name in param.multi_value:
                    yield scope_name

    def iter_scope_records(self) -> Iterable[Dict[str, Any]]:
        """
        Yield per-scope records for this event.

        Each yielded dict is one row useful for summarizing scopes.
        """
        event = self._first_event()
        if not event:
            return

        timestamp = self._parse_timestamp()
        unique_id = self.id.get("uniqueQualifier")

        # Preferred source: scope_data (with product_bucket)
        had_scope_data = False
        for msg in self._iter_scope_data_messages(event):
            had_scope_data = True
            scope_name = None
            product_bucket = None

            for p in msg.get("parameter", []):
                pname = p.get("name")
                if pname == "scope_name":
                    scope_name = p.get("value")
                elif pname == "product_bucket":
                    mv = p.get("multiValue") or []
                    product_bucket = mv[0] if mv else None

            if scope_name:
                service = self._derive_service_from_scope(scope_name)
                yield {
                    "timestamp": timestamp,
                    "unique_id": unique_id,
                    "scope_name": scope_name,
                    "product_bucket": product_bucket,
                    "service": service,
                    "is_readonly": scope_name.endswith(".readonly"),
                }

        # Fallback: plain scope list if scope_data is missing
        if not had_scope_data:
            for scope_name in self._iter_scope_list_values(event):
                service = self._derive_service_from_scope(scope_name)
                yield {
                    "timestamp": timestamp,
                    "unique_id": unique_id,
                    "scope_name": scope_name,
                    "product_bucket": None,
                    "service": service,
                    "is_readonly": scope_name.endswith(".readonly"),
                }


# --------------------------------------------------------------------------- #
# Aggregation structures                                                     #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ScopeKey:
    scope_name: str
    product_bucket: Optional[str]
    service: str
    is_readonly: bool


# --------------------------------------------------------------------------- #
# File reading & record iteration                                            #
# --------------------------------------------------------------------------- #


def iter_raw_objects_from_file(path: Path) -> Iterable[Dict[str, Any]]:
    """
    Yield raw JSON objects from a file.

    Supports:
      - NDJSON lines where each line is an activity object
      - NDJSON lines where each line is a top-level { "items": [...] } batch
      - Plain JSON containing { "items": [...] } or [ ... ] or single object
    """
    open_fn = gzip.open if path.suffix == ".gz" else open
    with open_fn(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                # Try treating the whole file as one JSON document
                f.seek(0)
                obj = json.load(f)
                return _iter_from_top_object(obj)

            # per-line object
            yield from _iter_from_top_object(obj)


def _iter_from_top_object(obj: Any) -> Iterable[Dict[str, Any]]:
    """Handle the various shapes of the top-level JSON object."""
    if isinstance(obj, dict) and "items" in obj:
        # Google Reports API batch response
        for item in obj["items"]:
            yield item
    elif isinstance(obj, list):
        for item in obj:
            yield item
    elif isinstance(obj, dict):
        # A single activity object
        yield obj
    else:
        # Unexpected shape; ignore
        return []


def iter_scope_keys_from_paths(paths: List[Path]) -> Iterable[ScopeKey]:
    """Yield ScopeKey for every scope found in the given files."""
    for path in paths:
        for obj in iter_raw_objects_from_file(path):
            try:
                act = RawTokenActivity.parse_obj(obj)
            except Exception:
                # Skip anything that doesn't conform to expected shape
                continue

            for scope_rec in act.iter_scope_records():
                yield ScopeKey(
                    scope_name=scope_rec["scope_name"],
                    product_bucket=scope_rec["product_bucket"],
                    service=scope_rec["service"],
                    is_readonly=scope_rec["is_readonly"],
                )


# --------------------------------------------------------------------------- #
# Main: aggregate & output                                                   #
# --------------------------------------------------------------------------- #


def collect_scopes(input_paths: List[Path]) -> Counter:
    """
    Return a Counter keyed by ScopeKey, counting occurrences across all input files.
    """
    counter: Counter = Counter()
    for key in iter_scope_keys_from_paths(input_paths):
        counter[key] += 1
    return counter


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect unique scopes from token logs.")
    parser.add_argument(
        "input",
        nargs="+",
        help="Input files or directories (NDJSON/JSON/JSON.gz).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Optional path to write CSV summary of scopes.",
    )

    args = parser.parse_args()

    # Expand dirs and glob patterns
    all_paths: List[Path] = []
    for inp in args.input:
        p = Path(inp)
        if p.is_dir():
            # pick common file types
            all_paths.extend(sorted(p.glob("*/*.jsonl.gz")))
        else:
            all_paths.append(p)

    if not all_paths:
        print("No input files found.")
        return

    print(f"Scanning {len(all_paths)} file(s)...")

    scope_counts = collect_scopes(all_paths)

    # Sort by count descending
    rows: List[Tuple[ScopeKey, int]] = sorted(scope_counts.items(), key=lambda kv: kv[1], reverse=True)

    print("\nTop scopes:")
    print(f"{'count':>8}  {'service':<14}  {'bucket':<14}  {'readonly':<8}  scope_name")
    print("-" * 100)
    for key, count in rows:
        print(
            f"{count:8d}  {key.service:<14}  {str(key.product_bucket or ''):<14}  "
            f"{str(key.is_readonly):<8}  {key.scope_name}"
        )

    if args.out:
        import csv

        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["scope_name", "product_bucket", "service", "is_readonly", "count"])
            for key, count in rows:
                writer.writerow(
                    [
                        key.scope_name,
                        key.product_bucket or "",
                        key.service,
                        "true" if key.is_readonly else "false",
                        count,
                    ]
                )
        print(f"\nWrote CSV summary to: {out_path}")


if __name__ == "__main__":
    main()
