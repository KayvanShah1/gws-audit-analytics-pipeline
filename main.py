from analyze import analyze as run_analysis
from config import get_logger
from fetcher import fetch_token_activity_buffered
from process import run as run_processing_pipeline
from utils import timed_run

logger = get_logger("TokenActivityOrchestrator")


@timed_run
def run_all():
    logger.info("Starting full pipeline...")
    fetch_token_activity_buffered()
    run_processing_pipeline()
    run_analysis()
    logger.info("Pipeline execution completed.")


if __name__ == "__main__":
    run_all()
