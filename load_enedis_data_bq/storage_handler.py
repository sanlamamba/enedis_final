import json
import logging
from typing import List, Dict, Any
from google.cloud import storage

logger = logging.getLogger(__name__)


def list_geojson_files(
    storage_client: storage.Client, bucket_name: str, prefix: str
) -> List[str]:
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    geojson_files = []
    for blob in blobs:
        if blob.name.endswith(".geojson"):
            geojson_files.append(blob.name)
            logger.info(f"Found: {blob.name} ({blob.size / (1024 * 1024):.2f} MB)")

    return geojson_files


def download_geojson(
    storage_client: storage.Client, bucket_name: str, blob_name: str
) -> Dict[str, Any]:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    content = blob.download_as_text()
    return json.loads(content)
