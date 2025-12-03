import json
import time
from datetime import datetime
from functools import wraps
from pathlib import Path

from gws_pipeline.core import get_logger

logger = get_logger("TimedRun")


def get_relative_path(file_path: str, base: str = Path.cwd()) -> str:
    """Get the relative path of file_path with respect to base.
    If file_path is not under base, return the absolute path.
    Args:
        file_path (str): The file path to convert
        base (str): The base path to relativize against
    Returns:
        str: Relative path if possible, else absolute path
    """
    try:
        return str(Path(file_path).relative_to(base))
    except ValueError:
        return str(Path(file_path))


def format_duration(seconds: float) -> str:
    """Format a duration given in seconds into a human-readable string.
    Args:
        seconds (float): Duration in seconds.
    Returns:
        str: Formatted duration string.
    """
    if seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    elif seconds < 60:
        return f"{seconds:.3f} s"
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} h {minutes:02} m {seconds:02} s"


def fetch_last_run_timestamp(path: str) -> datetime:
    """Fetch the last run timestamp from the state file."""
    with open(path, "r") as f:
        ts = datetime.fromisoformat(json.load(f)["last_run"])
    return ts


def timed_run(func):
    """Decorator to time the execution of a function and log its duration.
    Args:
        func (Callable): The function to be decorated.
    Returns:
        Callable: The wrapped function with timing.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_wall = time.perf_counter()
        start_cpu = time.process_time()
        logger.info("Started: %s", func.__name__)
        try:
            return func(*args, **kwargs)
        finally:
            wall = time.perf_counter() - start_wall
            cpu = time.process_time() - start_cpu
            logger.info("Completed %s in wall=%s cpu=%s", func.__name__, format_duration(wall), format_duration(cpu))

    return wrapper
