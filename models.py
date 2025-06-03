from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


# --- Pydantic model for API path and query params ---
class ActivityPathParams(BaseModel):
    userKey: str = "all"
    applicationName: str = "TOKEN"

    def get_path(self) -> str:
        return f"/admin/reports/v1/activity/users/{self.userKey}/applications/{self.applicationName}"


class ActivityQueryParams(BaseModel):
    startTime: datetime
    endTime: Optional[datetime] = None
    maxResults: int = 1000
    pageToken: Optional[str] = None

    def to_dict(self):
        return {
            k: v.isoformat() if isinstance(v, datetime) else v for k, v in self.model_dump(exclude_none=True).items()
        }


# --- Pydantic model for parsing and formatting token event responses ---
class EventParameter(BaseModel):
    name: str
    value: Optional[str] = None
    intValue: Optional[int] = None


class Event(BaseModel):
    name: str
    type: str
    parameters: List[EventParameter]


class RawTokenActivity(BaseModel):
    id: dict
    actor: dict
    events: List[Event]
    ipAddress: Optional[str] = None

    def to_flat_dict(self) -> dict:
        ts_raw = self.id.get("time")
        timestamp = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))

        result = {
            "timestamp": timestamp,
            "user": self.actor.get("email"),
            "profile_id": self.actor.get("profileId"),
            "unique_id": self.id.get("uniqueQualifier"),
            "ip": self.ipAddress,
            "event_type": None,
            "event_name": None,
            "method_name": None,
            "num_bytes": 0,
            "api_name": None,
            "client_id": None,
            "app_name": None,
            # "client_type": None,
            # "product_bucket": None,
        }

        if self.events:
            event = self.events[0]
            result["event_type"] = event.type
            result["event_name"] = event.name

            for param in event.parameters:
                if param.name == "method_name":
                    result["method_name"] = param.value
                elif param.name == "num_response_bytes":
                    result["num_bytes"] = param.intValue or 0
                elif param.name == "api_name":
                    result["api_name"] = param.value
                elif param.name == "client_id":
                    result["client_id"] = param.value
                elif param.name == "app_name":
                    result["app_name"] = param.value
                # elif param.name == "client_type":
                #     result["client_type"] = param.value
                # elif param.name == "product_bucket":
                #     result["product_bucket"] = param.value

        return result
