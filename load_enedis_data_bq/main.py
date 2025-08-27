import logging
from config import *
from loader import GeoJSONLoader
from bigquery_handler import ensure_dataset

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info(f"Starting GeoJSON loader for {PROJECT_ID}.{DATASET_ID}")

    loader = GeoJSONLoader(PROJECT_ID, DATASET_ID)
    ensure_dataset(loader.bq_client, PROJECT_ID, DATASET_ID, DATASET_LOCATION)

    date = "2025-08-25"
    prefix = f"processed/{date}/"

    loader.load_all(BUCKET_NAME, prefix)
    logger.info("Done!")


if __name__ == "__main__":
    main()
