from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, Field


class Application(str, Enum):
    TOKEN = "TOKEN"
    LOGIN = "login"
    SAML = "saml"
    ADMIN = "admin"
    DRIVE = "drive"


# --- Pydantic model for API path and query params ---
class ActivityPathParams(BaseModel):
    """Holds path parameters for Google Workspace Reports API.
    Values:
      - userKey: "all" by default
      - applicationName: e.g. "TOKEN", "login", "saml", "admin"
    """

    userKey: str = "all"
    applicationName: Application = Application.TOKEN

    def get_relative_path(self) -> str:
        return f"/admin/reports/v1/activity/users/{self.userKey}/applications/{self.applicationName.value}"

    def get_full_endpoint(self, base_url: str) -> str:
        """Returns full URL including base_url."""
        return f"{base_url}{self.get_relative_path()}"

    @classmethod
    def for_application(cls, application: Application, user_key: str = "all") -> "ActivityPathParams":
        """Convenience constructor for a given application and user key."""
        return cls(userKey=user_key, applicationName=application)


class ActivityQueryParams(BaseModel):
    """Query parameters for the Google Workspace Reports API token activity endpoint."""

    startTime: datetime
    endTime: Optional[datetime] = None
    maxResults: int = 1000
    pageToken: Optional[str] = None

    def to_dict(self):
        return {
            k: v.isoformat() if isinstance(v, datetime) else v for k, v in self.model_dump(exclude_none=True).items()
        }


# --- Pydantic models for run snapshots ---
class WindowRange(BaseModel):
    """Metadata for a single time window processed in a run."""

    idx: int = Field(..., description="Window index within the run (0-based)")
    start: datetime = Field(..., description="Window start time (API query start)")
    end: datetime = Field(..., description="Window end time (API query end)")


class RunSnapshot(BaseModel):
    """Lightweight per-run snapshot of what was fetched."""

    start: datetime = Field(..., description="Window start time (API query start)")
    end: datetime = Field(..., description="Window end time (API query end)")
    run_id: str = Field(..., description="Logical run identifier, e.g. run_start_time formatted")

    # Global event-time coverage for this run
    earliest_event_time: Optional[datetime] = Field(
        None, description="Earliest event_time seen across all windows in this run"
    )
    latest_event_time: Optional[datetime] = Field(
        None, description="Latest event_time seen across all windows in this run"
    )

    # Aggregate stats
    num_windows: int = Field(..., description="Number of windows processed")
    num_events: int = Field(..., description="Total events fetched in this run")

    # Per-window ranges (for your 2-day window / overlap analysis)
    windows: List[WindowRange] = Field(
        default_factory=list,
        description="List of window ranges and optional per-window stats",
    )

    model_config = {
        "json_encoders": {
            datetime: lambda dt: dt.isoformat(),
        }
    }


# --- Pydantic model for parsing and formatting token event responses ---
class EventParameter(BaseModel):
    """Parameter for a single event in token activity."""

    name: str
    value: Optional[str] = None
    int_value: Optional[int] = Field(default=None, alias="intValue")
    multi_value: Optional[List[str]] = Field(default=None, alias="multiValue")
    multi_message_value: Optional[List[Dict[str, Any]]] = Field(default=None, alias="multiMessageValue")


class Event(BaseModel):
    """Single event in token activity."""

    name: str
    type: str
    parameters: List[EventParameter]


class NetworkInfo(BaseModel):
    """Network information associated with a token activity event."""

    ip_asn: Optional[List[int]] = Field(default=None, alias="ipAsn")
    region_code: Optional[str] = Field(default=None, alias="regionCode")
    subdivision_code: Optional[str] = Field(default=None, alias="subdivisionCode")


class RawTokenActivity(BaseModel):
    """
    Raw token activity event as returned by the Google Workspace Reports API.

    This model provides:
    - to_event_record(): one flat row per event (for the main events table)
    - iter_scope_records(): one row per scope (for the scopes detail table)
    """

    id: dict
    actor: dict
    events: List[Event]
    ip_address: Optional[str] = Field(default=None, alias="ipAddress")
    network_info: Optional[NetworkInfo] = Field(default=None, alias="networkInfo")

    # --------------------------------------------------------------------- #
    # Classification maps
    # --------------------------------------------------------------------- #

    # Map product_bucket → coarse service
    BUCKET_SERVICE_MAP: ClassVar[Dict[str, str]] = {
        "GSUITE_ADMIN": "admin",
        "DRIVE": "drive",
        "GMAIL": "gmail",
        "APPS_SCRIPT_API": "apps_script",
        "APPS_SCRIPT_RUNTIME": "apps_script",
        "CALENDAR": "calendar",
        "IDENTITY": "identity",
        # "OTHER" intentionally omitted → fallback to scope-based logic
    }

    # Map scope "family" → coarse service (for when bucket is missing/OTHER)
    SCOPE_FAMILY_SERVICE_MAP: ClassVar[Dict[str, str]] = {
        # Google Workspace / Admin
        "admin": "admin",
        "apps": "admin",
        "cloud-identity": "admin",
        "cloudplatformprojects": "cloud_platform",
        # Product APIs
        "drive": "drive",
        "drive.addons": "drive",
        "datastudio": "datastudio",
        "spreadsheets": "sheets",
        "gmail": "gmail",
        "script": "apps_script",
        "sqlservice": "cloud_sql",
        "workspace": "workspace",  # workspace.linkpreview, etc.
        "calendar": "calendar",
        "calendar.addons": "calendar",
        # userinfo / OAuth
        "userinfo": "identity",
    }

    # --------------------------------------------------------------------- #
    # Helpers to build analytics-friendly records
    # --------------------------------------------------------------------- #

    def _parse_timestamp(self) -> datetime:
        ts_raw = self.id.get("time")
        # Example: "2025-11-27T03:11:11.616Z"
        # Normalize "Z" to "+00:00" for fromisoformat.
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

    @staticmethod
    def _normalize_non_google_scope(scope_name: str) -> str:
        """Handle generic / non-googleapis scopes."""
        simple = (scope_name or "").lower()

        if simple == "openid":
            return "identity"
        if simple in {
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.google.com/accounts/oauthlogin",
        }:
            return "identity"

        return "other"

    @classmethod
    def _extract_scope_family(cls, scope_name: str) -> Tuple[str, str]:
        """
        Extract (family, suffix) from scope_name.

        Examples:
          https://www.googleapis.com/auth/drive.readonly
            -> ('drive', 'readonly')
          https://www.googleapis.com/auth/admin.directory.user.readonly
            -> ('admin', 'directory.user.readonly')
          https://www.googleapis.com/auth/calendar.addons.current.event.read
            -> ('calendar.addons', 'current.event.read')
        """
        prefix = "https://www.googleapis.com/auth/"
        if not scope_name.startswith(prefix):
            return "", ""

        tail = scope_name[len(prefix) :]  # e.g. 'drive.readonly', 'admin.directory.user.readonly', ...

        parts = tail.split(".")
        # Special-case multi-word families like 'calendar.addons'
        if len(parts) >= 2 and parts[0] == "calendar" and parts[1] == "addons":
            family = "calendar.addons"
            suffix = ".".join(parts[2:]) if len(parts) > 2 else ""
        else:
            family = parts[0]
            suffix = ".".join(parts[1:]) if len(parts) > 1 else ""

        return family, suffix

    @classmethod
    def _derive_service(cls, scope_name: str, product_bucket: Optional[str]) -> str:
        """
        Coarse service classification:

          1. Use product_bucket if present and mapped
          2. Else classify by scope family
          3. Else fall back to generic non-google handler
        """
        # 1) Bucket-based classification
        if product_bucket:
            svc = cls.BUCKET_SERVICE_MAP.get(product_bucket)
            if svc:
                return svc

        # 2) Scope-name-based classification
        prefix = "https://www.googleapis.com/auth/"
        if scope_name and scope_name.startswith(prefix):
            family, _ = cls._extract_scope_family(scope_name)
            if family:
                svc = cls.SCOPE_FAMILY_SERVICE_MAP.get(family)
                if svc:
                    return svc

        # 3) Generic / non-google scopes
        return cls._normalize_non_google_scope(scope_name or "")

    # --------------------------------------------------------------------- #
    # Public API: event-level flat record (events table)
    # --------------------------------------------------------------------- #

    def to_event_record(self) -> Dict[str, Any]:
        """
        Return a single flat dict representing the event-level row.

        This is what you write to the main 'events' Parquet table.
        """
        timestamp = self._parse_timestamp()
        event = self._first_event()

        record: Dict[str, Any] = {
            # core identifiers
            "timestamp": timestamp,
            "unique_id": self.id.get("uniqueQualifier"),
            "user": self.actor.get("email"),
            "profile_id": self.actor.get("profileId"),
            # network info
            "ip": self.ip_address,
            "asn": None,
            "region_code": None,
            "subdivision_code": None,
            # event metadata
            "event_type": event.type if event else None,
            "event_name": event.name if event else None,
            "method_name": None,
            "num_bytes": 0,
            "api_name": None,
            "client_id": None,
            "app_name": None,
            "client_type": None,
            # aggregate scope info
            "scope_count": 0,
            "product_buckets": None,  # list[str] or null
            "has_drive_scope": False,
            "has_gmail_scope": False,
            "has_admin_scope": False,
        }

        # Network info
        if self.network_info:
            if self.network_info.ip_asn:
                record["asn"] = self.network_info.ip_asn[0]
            record["region_code"] = self.network_info.region_code
            record["subdivision_code"] = self.network_info.subdivision_code

        # Event parameters
        if event:
            for param in event.parameters:
                if param.name == "method_name":
                    record["method_name"] = param.value
                elif param.name == "num_response_bytes":
                    record["num_bytes"] = param.int_value or 0
                elif param.name == "api_name":
                    record["api_name"] = param.value
                elif param.name == "client_id":
                    record["client_id"] = param.value
                elif param.name == "app_name":
                    record["app_name"] = param.value
                elif param.name == "client_type":
                    record["client_type"] = param.value

        # Aggregate scopes and product buckets
        scopes = list(self.iter_scope_records())
        if scopes:
            record["scope_count"] = len(scopes)
            buckets = {s["product_bucket"] for s in scopes if s["product_bucket"]}
            if buckets:
                record["product_buckets"] = sorted(buckets)
                record["has_drive_scope"] = "DRIVE" in buckets
                record["has_gmail_scope"] = "GMAIL" in buckets
                record["has_admin_scope"] = "GSUITE_ADMIN" in buckets

        return record

    # --------------------------------------------------------------------- #
    # Public API: per-scope records (scopes table)
    # --------------------------------------------------------------------- #

    def iter_scope_records(self) -> Iterable[Dict[str, Any]]:
        """
        Yield per-scope records for this event.

        Each yielded dict is one row for the scopes detail table.
        """
        event = self._first_event()
        if not event:
            return

        unique_id = self.id.get("uniqueQualifier")
        timestamp = self._parse_timestamp()

        # Preferred source: scope_data (with product_bucket)
        had_scope_data = False
        for msg in self._iter_scope_data_messages(event):
            had_scope_data = True
            scope_name: Optional[str] = None
            product_bucket: Optional[str] = None

            for p in msg.get("parameter", []):
                pname = p.get("name")
                if pname == "scope_name":
                    scope_name = p.get("value")
                elif pname == "product_bucket":
                    mv = p.get("multiValue") or []
                    product_bucket = mv[0] if mv else None

            if scope_name:
                family, _ = self._extract_scope_family(scope_name)
                service = self._derive_service(scope_name, product_bucket)
                yield {
                    "timestamp": timestamp,
                    "unique_id": unique_id,
                    "scope_name": scope_name,
                    "scope_family": family or None,
                    "product_bucket": product_bucket,
                    "service": service,
                    "is_readonly": scope_name.endswith(".readonly"),
                }

        # Fallback: plain scope list if scope_data is missing
        if not had_scope_data:
            for scope_name in self._iter_scope_list_values(event):
                family, _ = self._extract_scope_family(scope_name)
                service = self._derive_service(scope_name, None)
                yield {
                    "timestamp": timestamp,
                    "unique_id": unique_id,
                    "scope_name": scope_name,
                    "scope_family": family or None,
                    "product_bucket": None,
                    "service": service,
                    "is_readonly": scope_name.endswith(".readonly"),
                }


if __name__ == "__main__":
    import json

    from gws_pipeline.core import settings
    from rich.pretty import pprint

    example_input = settings.base_dir / "input" / "example_response.json"

    records = []
    scope_records = []
    with open(example_input, "r") as f:
        data = json.loads(f.read())
        for item in data.get("items", []):
            event = RawTokenActivity(**item)
            records.append(event.to_event_record())
            for scope_rec in event.iter_scope_records():
                scope_records.append(scope_rec)

    pprint(scope_records)
    # pprint(records)
