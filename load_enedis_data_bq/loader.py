import logging
from typing import Optional
from google.cloud import storage, bigquery

from config import *
from storage_handler import list_geojson_files, download_geojson
from bigquery_handler import ensure_dataset, create_or_update_table, insert_rows
from data_processor import (
    process_features_batch,
    extract_date_from_path,
    get_table_name_from_path,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GeoJSONLoader:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)

    def load_file(
        self, bucket_name: str, blob_name: str, table_name: Optional[str] = None
    ):
        date = extract_date_from_path(blob_name)
        table_name = table_name or get_table_name_from_path(blob_name)

        logger.info(
            f"Loading {blob_name} -> {table_name} and enedis_full (date: {date})"
        )

        data = download_geojson(self.storage_client, bucket_name, blob_name)
        features = data.get("features", [])

        if not features:
            logger.warning(f"No features in {blob_name}")
            return

        logger.info(f"Processing {len(features)} features")

        first_batch = next(
            process_features_batch(
                features[:MAX_SCHEMA_FEATURES], date, MAX_SCHEMA_FEATURES
            )
        )

        table = create_or_update_table(
            self.bq_client, self.project_id, self.dataset_id, table_name, first_batch
        )

        first_batch_with_source = [
            {**row, "source_table": table_name} for row in first_batch
        ]
        full_table = create_or_update_table(
            self.bq_client,
            self.project_id,
            self.dataset_id,
            "enedis_full",
            first_batch_with_source,
        )

        total_inserted = 0
        for batch in process_features_batch(features, date, CHUNK_SIZE):
            if total_inserted > 0 and total_inserted % 50000 == 0:
                table = self.bq_client.get_table(table.reference)
                full_table = self.bq_client.get_table(full_table.reference)

            insert_rows(self.bq_client, table, batch)

            batch_with_source = [{**row, "source_table": table_name} for row in batch]
            insert_rows(self.bq_client, full_table, batch_with_source)

            total_inserted += len(batch)
            logger.info(
                f"Progress: {total_inserted}/{len(features)} (inserted to both {table_name} and enedis_full)"
            )

        logger.info(f"Completed: {total_inserted} rows inserted to both tables")

    def load_all(self, bucket_name: str, prefix: str):
        files = list_geojson_files(self.storage_client, bucket_name, prefix)

        if not files:
            logger.warning(f"No GeoJSON files found in {bucket_name}/{prefix}")
            return

        logger.info(f"Processing {len(files)} files")

        for blob_name in files:
            try:
                self.load_file(bucket_name, blob_name)
            except Exception as e:
                logger.error(f"Failed to process {blob_name}: {e}")
                continue

        logger.info("All files processed")
