from gws_pipeline.core.analyze import analyze as run_analysis
from gws_pipeline.core import get_logger
from gws_pipeline.core.fetcher import fetch_token_activity_buffered
from gws_pipeline.core.process import process_recent_token_activity_beam
from gws_pipeline.core.utils import timed_run

logger = get_logger("TokenActivityOrchestrator")


@timed_run
def run_all():
    logger.info("Starting full pipeline...")
    fetch_token_activity_buffered()
    process_recent_token_activity_beam()
    run_analysis()
    logger.info("Pipeline execution completed.")


if __name__ == "__main__":
    run_all()
