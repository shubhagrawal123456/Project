from __future__ import annotations

import logging
import time

from bing_ads_etl.config import get_settings
from bing_ads_etl.extract import extract
from bing_ads_etl.load import apply_schema, connect, load
from bing_ads_etl.transform import transform
from bing_ads_etl.validate import validate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _wait_for_mysql(settings, attempts: int = 30) -> None:
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            conn = connect(settings)
            conn.close()
            return
        except Exception as exc:
            last_error = exc
            logger.warning("MySQL not ready: %s", exc)
            time.sleep(2)
    raise RuntimeError(f"MySQL was not reachable at {settings.mysql_host}:{settings.mysql_port}") from last_error


def run_pipeline() -> dict[str, int]:
    settings = get_settings()
    logger.info("Extracting %s", settings.source_csv)
    extract(settings)
    logger.info("Validating extracted rows")
    validate(settings)
    logger.info("Building star-schema tables")
    transform(settings)
    logger.info("Waiting for MySQL")
    _wait_for_mysql(settings)
    logger.info("Applying warehouse DDL")
    apply_schema(settings)
    counts = load(settings)
    logger.info("Load complete: %s", counts)
    return counts


if __name__ == "__main__":
    run_pipeline()
