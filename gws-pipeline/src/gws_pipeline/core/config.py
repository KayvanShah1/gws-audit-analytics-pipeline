import logging
import logging.handlers
import os
from datetime import timezone
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from rich.logging import RichHandler


class AppSettings(BaseSettings):
    # Base directories
    base_dir: Path = Path(__file__).resolve().parents[4]

    # File paths for data storage
    var_dir: Path = Path.joinpath(base_dir, "var")
    if not var_dir.exists():
        var_dir.mkdir(parents=True, exist_ok=True)

    # Data directories
    data_dir: Path = Path.joinpath(var_dir, "data")
    raw_data_dir: Path = Path.joinpath(data_dir, "raw")
    per_run_data_dir: Path = Path.joinpath(data_dir, "runs_snapshots")
    processed_data_dir: Path = Path.joinpath(data_dir, "processed")

    # Log and state directories
    state_dir: Path = Path.joinpath(var_dir, "state")
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir: Path = Path.joinpath(var_dir, "logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # Fetcher and processing settings
    BUFFER_SIZE: int = Field(5000, description="Buffer size for batch partition flush")
    PER_RUN_BUFFER_SIZE: int = Field(2500, description="Buffer size for per-run log file")
    DEFAULT_TIMEZONE: timezone = Field(timezone.utc, description="Timezone for datetime handling")
    DEFAULT_DATE_FORMAT: str = Field("%Y-%m-%dT%H:%M:%S%z", description="Format for datetime parsing")
    DEFAULT_DELTA_HRS: int = Field(48, description="Default hours back to fetch on first run")
    BACKWARD_OVERLAP_MINUTES: int = Field(3, description="Overlap buffer for safe backfill")
    USE_GZIP: bool = Field(False, description="Whether to gzip output files")
    GZIP_COMPRESSION_LVL: int = Field(5, description="Compression level for gzip files")
    MAX_PARALLEL_WINDOWS: int = Field(4, description="Maximum number of parallel windows")
    WINDOW_HOURS: int = Field(6, description="Hours per fetch window")
    WRITE_SNAPSHOT: bool = Field(True, description="Whether to write per-run snapshots")
    PROCESS_BATCH_DAYS: int = Field(2, description="Days to process in each batch run")

    # MotherDuck settings
    MOTHERDUCK_TOKEN: str = Field(..., description="MotherDuck authentication token")
    MOTHERDUCK_DB_NAME: str = Field(..., description="MotherDuck database name")

    # DuckDB Schema
    DUCKDB_LOADER_SCHEMA: str = Field("processed", description="DuckDB schema for loader tables")

    # Google Workspace API settings
    base_url: str = Field("https://www.googleapis.com", description="Base URL for Google Workspace API")
    subject: str = Field(..., description="Subject email for impersonation in API requests")
    creds_file: Path = Field(
        default=Path.joinpath(base_dir, "creds.json"), description="Path to Google Workspace credentials file"
    )

    # Configuration for Pydantic settings
    model_config = SettingsConfigDict(
        env_prefix="GWS_", env_file=f"{base_dir}/.env", env_file_encoding="utf-8", extra="ignore"
    )

    # Override model_dump to make paths relative to base_dir for logging
    def model_dump(self, **kwargs):
        dump = super().model_dump(**kwargs)
        for k, v in dump.items():
            if isinstance(v, Path) and v.is_absolute():
                try:
                    dump[k] = v.relative_to(self.base_dir)
                except ValueError:
                    pass
        return dump

    @property
    def duckdb_connection_string(self) -> str:
        return f"md:{self.MOTHERDUCK_DB_NAME}?motherduck_token={self.MOTHERDUCK_TOKEN}"


settings = AppSettings()


def get_logger(name):
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Console handler (Rich)
        ch = RichHandler(rich_tracebacks=True)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter("'%(name)s' - %(message)s"))
        logger.addHandler(ch)

        # File handler (Rotating)
        fh = logging.handlers.RotatingFileHandler(
            os.path.join(settings.log_dir, "gws-activity-analyzer.log"),
            maxBytes=128 * 1024,  # 20KB log file max
            backupCount=3,
            delay=True,
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    from rich.pretty import pretty_repr

    logger = get_logger("GWSSettings")
    s = pretty_repr(settings.model_dump())
    logger.info(f"Settings loaded: \n{s}")  # Debugging line to check settings
    logger.info(f"DuckDB connection string: {settings.duckdb_connection_string}")  # Debugging line to check settings
