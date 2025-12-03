from datetime import datetime
from pathlib import Path
from typing import List

from dagster import ConfigurableResource
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from gws_pipeline.core import settings
from gws_pipeline.core.fetcher import load_last_run_timestamp, save_last_run_timestamp
from gws_pipeline.core.models import Application


class GoogleReportsAPIResource(ConfigurableResource):
    service_account_file: str = str(settings.creds_file)
    subject: str = settings.subject
    scopes: List[str] = ["https://www.googleapis.com/auth/admin.reports.audit.readonly"]
    backoff_factor: float = 1.0
    total_retries: int = 5

    def get_session(self) -> AuthorizedSession:
        creds = Credentials.from_service_account_file(
            self.service_account_file,
            scopes=self.scopes,
            subject=self.subject,
        )
        session = AuthorizedSession(creds)
        retry_strategy = Retry(
            total=self.total_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
            raise_on_redirect=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session


class StateFileResource(ConfigurableResource):
    def path_obj(self, application: Application) -> Path:
        return settings.state_dir / f"{application.value.lower()}.json"

    def load_last_run(self, application: Application) -> datetime:
        return load_last_run_timestamp(self.path_obj(application))

    def save_last_run(self, ts: datetime, application: Application):
        save_last_run_timestamp(self.path_obj(application), ts)
