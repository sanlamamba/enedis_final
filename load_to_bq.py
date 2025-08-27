"""
===============================================================================
                          GEOJSON TO BIGQUERY LOADER
===============================================================================

Ce module permet de charger des fichiers GeoJSON depuis Google Cloud Storage
vers des tables BigQuery. Il automatise le processus de conversion des données
géospatiales en format BigQuery avec gestion automatique du schéma.

FONCTIONNALITÉS PRINCIPALES:
- Découverte automatique des fichiers GeoJSON dans GCS
- Conversion des features GeoJSON en lignes BigQuery
- Génération automatique du schéma de table
- Création/mise à jour des tables BigQuery
- Gestion des types de géométrie (Point, Polygon, etc.)
- Partitionnement automatique par date
- Nettoyage des noms de colonnes

WORKFLOW:
1. Liste les fichiers GeoJSON dans un bucket/préfixe GCS
2. Télécharge et parse chaque fichier
3. Convertit les features en lignes BigQuery
4. Infère le schéma depuis les données
5. Crée ou met à jour la table BigQuery
6. Insère les données avec gestion d'erreurs

STRUCTURE DES TABLES GÉNÉRÉES:
- Propriétés GeoJSON → colonnes BigQuery (noms nettoyés)
- Géométrie stockée en JSON string
- Colonnes latitude/longitude pour les points
- Métadonnées: source_date, load_timestamp
- Partitionnement par source_date

USAGE:
loader = GeoJSONToBigQuery(PROJECT_ID, DATASET_ID)
loader.process_all_files(BUCKET_NAME, "processed/2024-01-15/")

DÉPENDANCES:
- google-cloud-storage
- google-cloud-bigquery

===============================================================================
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Iterator

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# =============================================================================
# CONFIGURATION GLOBALE
# =============================================================================

PROJECT_ID = "ofr-2kt-valo-reseau-1-lab-prd"
DATASET_ID = "ofr_2kt_enedis"
BUCKET_NAME = "ofr-2kt-valo-enedis"
DATASET_LOCATION = "EU"
CHUNK_SIZE = 5000
MAX_SCHEMA_FEATURES = 100

# =============================================================================
# CONFIGURATION LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# CLASSE PRINCIPALE
# =============================================================================
class GeoJSONToBigQuery:

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)

    def list_geojson_files(self, bucket_name: str, prefix: str) -> List[str]:
        logger.info(f"Listing files in bucket: {bucket_name}, prefix: {prefix}")
        bucket = self.storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        geojson_files = []
        total_blobs = 0
        for blob in blobs:
            total_blobs += 1
            if blob.name.endswith(".geojson"):
                file_size = blob.size / (1024 * 1024)
                geojson_files.append(blob.name)
                logger.info(f"Found GeoJSON file: {blob.name} ({file_size:.2f} MB)")

        logger.info(
            f"Scanned {total_blobs} total files, found {len(geojson_files)} GeoJSON files"
        )
        return geojson_files

    def download_geojson(self, bucket_name: str, blob_name: str) -> Dict[str, Any]:
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.reload()
        file_size_mb = blob.size / (1024 * 1024)
        logger.info(f"Starting download: {blob_name} ({file_size_mb:.2f} MB)")

        start_time = time.time()
        content = blob.download_as_text()
        download_time = time.time() - start_time
        logger.info(f"Download completed in {download_time:.2f}s")

        logger.info("Parsing JSON content...")
        start_parse = time.time()
        data = json.loads(content)
        parse_time = time.time() - start_parse
        logger.info(f"JSON parsing completed in {parse_time:.2f}s")

        return data

    def process_features_in_chunks(
        self, features: List[Dict], date: str
    ) -> Iterator[List[Dict[str, Any]]]:
        """Traite les features par chunks pour éviter les problèmes de mémoire"""
        for i in range(0, len(features), CHUNK_SIZE):
            chunk = features[i : i + CHUNK_SIZE]
            rows = []

            for feature in chunk:
                row = {}

                properties = feature.get("properties", {})
                for key, value in properties.items():
                    clean_key = key.replace("-", "_").replace(" ", "_").lower()

                    if isinstance(value, list):
                        if all(isinstance(item, (str, int, float)) for item in value):
                            row[clean_key] = ", ".join(str(item) for item in value)
                        else:
                            row[clean_key] = json.dumps(value)
                    else:
                        row[clean_key] = value

                geometry = feature.get("geometry", {})
                if geometry:
                    row["geometry"] = json.dumps(geometry)
                    row["geometry_type"] = geometry.get("type")

                    if geometry.get("type") == "Point" and geometry.get("coordinates"):
                        coords = geometry.get("coordinates")
                        if len(coords) >= 2:
                            row["longitude"] = coords[0]
                            row["latitude"] = coords[1]

                row["source_date"] = date
                row["load_timestamp"] = datetime.now(timezone.utc).isoformat()

                rows.append(row)

            logger.info(f"Processed chunk {i//CHUNK_SIZE + 1}: {len(rows)} rows")
            yield rows

    def create_schema_from_sample(
        self, features: List[Dict]
    ) -> List[bigquery.SchemaField]:
        """Crée le schéma en utilisant un échantillon limité de features"""
        sample_features = features[:MAX_SCHEMA_FEATURES]
        sample_rows = []

        for feature in sample_features:
            row = {}

            properties = feature.get("properties", {})
            for key, value in properties.items():
                clean_key = key.replace("-", "_").replace(" ", "_").lower()

                if isinstance(value, list):
                    if all(isinstance(item, (str, int, float)) for item in value):
                        row[clean_key] = ", ".join(str(item) for item in value)
                    else:
                        row[clean_key] = json.dumps(value)
                else:
                    row[clean_key] = value

            geometry = feature.get("geometry", {})
            if geometry:
                row["geometry"] = json.dumps(geometry)
                row["geometry_type"] = geometry.get("type")

                if geometry.get("type") == "Point" and geometry.get("coordinates"):
                    coords = geometry.get("coordinates")
                    if len(coords) >= 2:
                        row["longitude"] = coords[0]
                        row["latitude"] = coords[1]

            row["source_date"] = "2025-01-01"
            row["load_timestamp"] = datetime.now(timezone.utc).isoformat()

            sample_rows.append(row)

        return self.create_table_schema(sample_rows)

    def create_table_schema(
        self, sample_rows: List[Dict[str, Any]]
    ) -> List[bigquery.SchemaField]:
        schema_dict = {}

        for row in sample_rows:
            for key, value in row.items():
                if key not in schema_dict:
                    if isinstance(value, str) and len(value) == 10:
                        try:
                            datetime.strptime(value, "%Y-%m-%d")
                            schema_dict[key] = "DATE"
                            continue
                        except ValueError:
                            pass
                    if isinstance(value, bool):
                        schema_dict[key] = "BOOLEAN"
                    elif isinstance(value, int):
                        schema_dict[key] = "INTEGER"
                    elif isinstance(value, float):
                        schema_dict[key] = "FLOAT"
                    elif isinstance(value, str):
                        schema_dict[key] = "STRING"
                    else:
                        schema_dict[key] = "STRING"

        schema = []
        for field_name, field_type in schema_dict.items():
            schema.append(bigquery.SchemaField(field_name, field_type, mode="NULLABLE"))

        return schema

    def create_or_update_table(
        self, table_name: str, schema: List[bigquery.SchemaField]
    ) -> bigquery.Table:
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        try:
            table = self.bq_client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")

            existing_fields = {field.name for field in table.schema}
            new_fields = []

            for field in schema:
                if field.name not in existing_fields:
                    new_fields.append(field)

            if new_fields:
                logger.info(f"Adding {len(new_fields)} new fields to table schema")
                table.schema = table.schema + new_fields
                table = self.bq_client.update_table(table, ["schema"])

        except NotFound:
            logger.info(f"Creating new table {table_id}")
            table = bigquery.Table(table_id, schema=schema)

            table.description = f"GeoJSON data from {table_name} files"
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="source_date"
            )

            table = self.bq_client.create_table(table)
            time.sleep(3)

        return table

    def load_data_to_bigquery(
        self, table: bigquery.Table, rows: List[Dict[str, Any]]
    ) -> None:
        if not rows:
            logger.warning("No rows to insert")
            return

        errors = self.bq_client.insert_rows_json(table, rows)

        if errors:
            logger.error(f"Failed to insert rows: {errors}")
            raise Exception(f"BigQuery insertion failed: {errors}")
        else:
            logger.info(f"Successfully inserted {len(rows)} rows into {table.table_id}")

    def process_geojson_file(
        self, bucket_name: str, blob_name: str, table_name: str = None
    ) -> None:
        path_parts = blob_name.split("/")
        date = None
        if len(path_parts) >= 2:
            potential_date = path_parts[1]
            try:
                datetime.strptime(potential_date, "%Y-%m-%d")
                date = potential_date
            except ValueError:
                date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        else:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if not table_name:
            filename = blob_name.split("/")[-1]
            table_name = filename.replace(".geojson", "").replace("-", "_").lower()

        logger.info(f"Processing {blob_name} for table {table_name} with date {date}")

        geojson_data = self.download_geojson(bucket_name, blob_name)
        features = geojson_data.get("features", [])

        if not features:
            logger.warning(f"No features found in {blob_name}")
            return

        total_features = len(features)
        logger.info(
            f"Found {total_features} features, processing in chunks of {CHUNK_SIZE}"
        )

        schema = self.create_schema_from_sample(features)
        table = self.create_or_update_table(table_name, schema)

        total_inserted = 0
        for chunk_rows in self.process_features_in_chunks(features, date):
            self.load_data_to_bigquery(table, chunk_rows)
            total_inserted += len(chunk_rows)

            # Libérer la mémoire
            del chunk_rows

            logger.info(f"Progress: {total_inserted}/{total_features} rows inserted")

        logger.info(f"Completed processing {blob_name}: {total_inserted} rows inserted")

    def process_all_files(self, bucket_name: str, prefix: str) -> None:
        geojson_files = self.list_geojson_files(bucket_name, prefix)

        if not geojson_files:
            logger.warning(f"No GeoJSON files found in {bucket_name}/{prefix}")
            return

        logger.info(f"Found {len(geojson_files)} GeoJSON files to process")

        for blob_name in geojson_files:
            try:
                self.process_geojson_file(bucket_name, blob_name)
            except Exception as e:
                logger.error(f"Failed to process {blob_name}: {str(e)}")
                continue

        logger.info("Completed processing all files")


# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================


def main():
    logger.info("🚀 GEOJSON TO BIGQUERY LOADER STARTING")
    logger.info(
        f"Configuration: Project={PROJECT_ID}, Dataset={DATASET_ID}, Bucket={BUCKET_NAME}"
    )

    bq_client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"

    logger.info("Checking dataset existence...")
    try:
        bq_client.get_dataset(dataset_id)
        logger.info(f"✅ Dataset {dataset_id} already exists")
    except NotFound:
        logger.info(f"📝 Creating dataset {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = DATASET_LOCATION
        dataset = bq_client.create_dataset(dataset, timeout=30)
        logger.info(f"✅ Dataset created successfully")

    logger.info("Initializing loader...")
    loader = GeoJSONToBigQuery(PROJECT_ID, DATASET_ID)

    date = "2025-08-25"
    prefix = f"processed/{date}/"

    logger.info(f"Starting processing for date: {date}")
    logger.info(f"Looking for files with prefix: {prefix}")

    start_time = time.time()
    loader.process_all_files(BUCKET_NAME, prefix)
    total_time = time.time() - start_time

    logger.info(f"🎉 ALL PROCESSING COMPLETED in {total_time:.2f}s")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
