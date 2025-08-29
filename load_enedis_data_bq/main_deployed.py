import functions_framework
import logging
from config import *
from loader import GeoJSONLoader
from bigquery_handler import ensure_dataset
from data_processor import extract_date_from_path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def process_enedis_gcs(cloud_event):
    """Process GCS events for ENEDIS data loading"""

    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]

    logger.info(f"Event ID: {event_id}")
    logger.info(f"Event type: {event_type}")
    logger.info(f"Bucket: {bucket}")
    logger.info(f"File: {name}")

    if "finalize" not in event_type:
        logger.info(f"Ignoring event type: {event_type}")
        return

    if not name.startswith("processed/") or not name.endswith(".geojson"):
        logger.info(f"Ignoring file: {name}")
        return

    try:
        date = extract_date_from_path(name)
        prefix = f"processed/{date}/"
        logger.info(f"Extracted date: {date}, processing prefix: {prefix}")
    except Exception as e:
        logger.error(f"Failed to extract date from {name}: {e}")
        return

    if bucket != BUCKET_NAME:
        logger.info(f"Ignoring bucket: {bucket} (expected: {BUCKET_NAME})")
        return

    try:
        logger.info(f"Starting ENEDIS data load for date: {date}")

        loader = GeoJSONLoader(PROJECT_ID, DATASET_ID)
        ensure_dataset(loader.bq_client, PROJECT_ID, DATASET_ID, DATASET_LOCATION)

        loader.load_all(bucket, prefix)

        logger.info(f"Successfully completed ENEDIS data load for date: {date}")

    except Exception as e:
        logger.error(f"Failed to process ENEDIS data for date {date}: {e}")
        raise
