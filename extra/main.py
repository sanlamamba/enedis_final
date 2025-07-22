import os
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional
from flask import Flask, jsonify, request
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
PROJECT_ID = "ofr-2kt-valo-reseau-1-lab-prd"
BUCKET_NAME = "ofr-2kt-valo-enedis"
FOLDER_PATH = "processed"
DATASET_ID = "ofr_2kt_enedis"
ERROR_LOG_FILE = "geojson_processing_errors.log"

# BigQuery load job configuration
LOAD_JOB_CONFIG = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    max_bad_records=10,
    ignore_unknown_values=True,
)

# Initialize clients with explicit project
logger.info(f"Initializing clients for project: {PROJECT_ID}")
try:
    storage_client = storage.Client(project=PROJECT_ID)
    bigquery_client = bigquery.Client(project=PROJECT_ID)
    logger.info("Successfully initialized GCP clients")
except Exception as e:
    logger.error(f"Failed to initialize GCP clients: {str(e)}")
    raise


def log_error_to_bucket(error_msg: str):
    """Log errors to a file in the bucket root."""
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(ERROR_LOG_FILE)

        # Get existing log content if exists
        existing_content = ""
        try:
            existing_content = blob.download_as_text()
        except NotFound:
            logger.info("No existing error log file, creating new one")

        # Append new error with timestamp
        timestamp = datetime.utcnow().isoformat()
        new_entry = f"\n[{timestamp}] {error_msg}\n"
        updated_content = existing_content + new_entry

        # Upload updated log
        blob.upload_from_string(updated_content)
        logger.info(f"Error logged to bucket: {error_msg[:100]}...")
    except Exception as e:
        logger.error(f"Failed to log error to bucket: {str(e)}")
        logger.error(f"Original error was: {error_msg}")


def safe_get_value(obj: Dict, path: str, default=None):
    """Safely get nested dictionary values."""
    try:
        keys = path.split(".")
        value = obj
        for key in keys:
            value = value.get(key, default)
            if value is None:
                return default
        return value
    except Exception:
        return default


def infer_schema_from_geojson(
    features: List[Dict[str, Any]],
) -> List[bigquery.SchemaField]:
    """Infer BigQuery schema from GeoJSON features."""
    logger.info(f"Inferring schema from {len(features)} features")

    schema_fields = [
        bigquery.SchemaField("feature_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("feature_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geometry_json", "JSON", mode="NULLABLE"),
        bigquery.SchemaField("geometry", "GEOGRAPHY", mode="NULLABLE"),
    ]

    # Analyze properties from all features to determine complete schema
    property_types = {}
    property_samples = {}

    for i, feature in enumerate(features):
        if i % 100 == 0:
            logger.info(f"Analyzing feature {i}/{len(features)}")

        if "properties" in feature and isinstance(feature["properties"], dict):
            for key, value in feature["properties"].items():
                if key not in property_samples:
                    property_samples[key] = []
                property_samples[key].append(value)

                # Determine type based on all occurrences
                if key not in property_types:
                    if value is None:
                        property_types[key] = ("STRING", "NULLABLE")
                    elif isinstance(value, bool):
                        property_types[key] = ("BOOLEAN", "NULLABLE")
                    elif isinstance(value, int) and not isinstance(value, bool):
                        property_types[key] = ("INTEGER", "NULLABLE")
                    elif isinstance(value, float):
                        property_types[key] = ("FLOAT", "NULLABLE")
                    elif isinstance(value, list):
                        # Check if it's a list of strings
                        if all(
                            isinstance(item, str) for item in value if item is not None
                        ):
                            property_types[key] = ("STRING", "REPEATED")
                        else:
                            property_types[key] = ("JSON", "NULLABLE")
                    elif isinstance(value, dict):
                        property_types[key] = ("JSON", "NULLABLE")
                    else:
                        property_types[key] = ("STRING", "NULLABLE")

    # Add property fields to schema
    logger.info(f"Found {len(property_types)} unique properties")
    for field_name, (field_type, field_mode) in sorted(property_types.items()):
        # Clean field name to be BigQuery compatible
        clean_name = field_name.replace("-", "_").replace(" ", "_").replace(".", "_")
        logger.debug(f"Adding field: {clean_name} ({field_type}, {field_mode})")
        schema_fields.append(
            bigquery.SchemaField(f"{clean_name}", field_type, mode=field_mode)
        )

    return schema_fields


def convert_geometry_to_wkt(geometry: Dict[str, Any]) -> Optional[str]:
    """Convert GeoJSON geometry to WKT format."""
    try:
        geom_type = geometry.get("type", "").upper()
        coords = geometry.get("coordinates", [])

        if geom_type == "POINT" and len(coords) >= 2:
            return f"POINT({coords[0]} {coords[1]})"

        elif geom_type == "LINESTRING" and coords:
            # Ajout de virgules entre les paires de coordonnées
            points = ", ".join([f"{c[0]} {c[1]}" for c in coords if len(c) >= 2])
            return f"LINESTRING({points})" if points else None

        elif geom_type == "POLYGON" and coords:
            rings = []
            for ring in coords:
                if ring:
                    # Ajout de virgules entre les paires de coordonnées
                    points = ", ".join([f"{c[0]} {c[1]}" for c in ring if len(c) >= 2])
                    if points:
                        rings.append(f"({points})")
            if rings:
                return f"POLYGON({', '.join(rings)})"

        elif geom_type == "MULTIPOINT" and coords:
            # Ajout de virgules entre les points
            points = ", ".join([f"({c[0]} {c[1]})" for c in coords if len(c) >= 2])
            return f"MULTIPOINT({points})" if points else None

        elif geom_type == "MULTILINESTRING" and coords:
            linestrings = []
            for ls in coords:
                if ls:
                    # Ajout de virgules entre les paires de coordonnées
                    points = ", ".join([f"{c[0]} {c[1]}" for c in ls if len(c) >= 2])
                    if points:
                        linestrings.append(f"({points})")
            if linestrings:
                return f"MULTILINESTRING({', '.join(linestrings)})"

        elif geom_type == "MULTIPOLYGON" and coords:
            polygons = []
            for polygon in coords:
                rings = []
                for ring in polygon:
                    if ring:
                        # Ajout de virgules entre les paires de coordonnées
                        points = ", ".join(
                            [f"{c[0]} {c[1]}" for c in ring if len(c) >= 2]
                        )
                        if points:
                            rings.append(f"({points})")
                if rings:
                    polygons.append(f"({', '.join(rings)})")
            if polygons:
                return f"MULTIPOLYGON({', '.join(polygons)})"

    except Exception as e:
        logger.warning(f"Failed to convert geometry to WKT: {str(e)}")

    return None


def convert_geojson_to_bigquery_rows(
    geojson_data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Convert GeoJSON features to BigQuery-compatible rows."""
    rows = []
    features = geojson_data.get("features", [])

    logger.info(f"Converting {len(features)} features to BigQuery rows")

    for i, feature in enumerate(features):
        if i % 100 == 0:
            logger.info(f"Processing feature {i}/{len(features)}")
        try:
            row = {
                "feature_id": str(feature.get("id", f"feature_{i}")),
                "feature_type": feature.get("type", "Feature"),
                "geometry_json": (
                    json.dumps(feature.get("geometry", {}))
                    if feature.get("geometry")
                    else None
                ),
            }

            # Convert geometry to WKT for GEOGRAPHY type
            geometry = feature.get("geometry")
            if geometry:
                wkt = convert_geometry_to_wkt(geometry)
                if wkt:
                    row["geometry"] = wkt
                else:
                    logger.warning(f"Could not convert geometry for feature {i}")
                    row["geometry"] = None
            else:
                row["geometry"] = None

            # Flatten properties
            properties = feature.get("properties", {})
            if isinstance(properties, dict):
                for key, value in properties.items():
                    # Clean field name
                    clean_key = (
                        key.replace("-", "_").replace(" ", "_").replace(".", "_")
                    )
                    field_name = f"{clean_key}"

                    # Handle different value types
                    if value is None:
                        row[field_name] = None
                    elif isinstance(value, (list, dict)):
                        # For complex types, keep as is (will be JSON)
                        row[field_name] = value
                    else:
                        row[field_name] = value

            rows.append(row)

        except Exception as e:
            logger.error(f"Error processing feature {i}: {str(e)}")
            logger.debug(f"Feature data: {json.dumps(feature)[:200]}...")

    logger.info(f"Successfully converted {len(rows)} rows")
    return rows


def load_data_to_bigquery(
    table_id: str, rows: List[Dict[str, Any]], schema: List[bigquery.SchemaField]
) -> bool:
    """Load data to BigQuery using the streaming API in batches."""
    try:
        logger.info(f"Loading {len(rows)} rows to {table_id}")

        # Insert rows in batches to avoid timeouts
        batch_size = 500
        total_errors = []

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            logger.info(
                f"Inserting batch {i//batch_size + 1}/{(len(rows) + batch_size - 1)//batch_size}"
            )

            errors = bigquery_client.insert_rows_json(table_id, batch)
            if errors:
                logger.error(f"Errors in batch {i//batch_size + 1}: {errors}")
                total_errors.extend(errors)

            # Small delay to avoid overwhelming the API
            time.sleep(0.1)

        if total_errors:
            logger.error(f"Total errors during insertion: {len(total_errors)}")
            return False

        logger.info(f"Successfully loaded all {len(rows)} rows")
        return True

    except Exception as e:
        logger.error(f"Failed to load data to BigQuery: {str(e)}")
        return False


def process_geojson_file(bucket_name: str, blob_name: str) -> Dict[str, Any]:
    """Process a single GeoJSON file and load it into BigQuery."""
    result = {
        "file": blob_name,
        "status": "success",
        "rows_processed": 0,
        "error": None,
        "processing_time": 0,
    }

    start_time = time.time()

    try:
        logger.info(f"Starting processing of {blob_name}")

        # Download GeoJSON file
        logger.info(f"Downloading file from gs://{bucket_name}/{blob_name}")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Check file size
        blob.reload()
        file_size_mb = blob.size / (1024 * 1024)
        logger.info(f"File size: {file_size_mb:.2f} MB")

        geojson_content = blob.download_as_text()
        logger.info(f"Downloaded {len(geojson_content)} characters")

        # Parse GeoJSON
        logger.info("Parsing GeoJSON data")
        geojson_data = json.loads(geojson_content)
        features = geojson_data.get("features", [])

        if not features:
            raise ValueError("No features found in GeoJSON")

        logger.info(f"Found {len(features)} features in GeoJSON")

        # Extract table name from filename
        filename = os.path.basename(blob_name)
        table_name = os.path.splitext(filename)[0]
        table_name = table_name.replace("-", "_").replace(" ", "_").lower()
        logger.info(f"Target table name: {table_name}")

        # Infer schema
        logger.info("Inferring schema from features")
        schema = infer_schema_from_geojson(features)
        logger.info(f"Schema has {len(schema)} fields")

        # Convert features to BigQuery rows
        logger.info("Converting features to BigQuery format")
        rows = convert_geojson_to_bigquery_rows(geojson_data)

        if not rows:
            raise ValueError("No rows were converted from features")

        # Create or replace table
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        # Delete table if exists
        try:
            bigquery_client.delete_table(table_id)
            logger.info(f"Deleted existing table: {table_id}")
        except NotFound:
            logger.info(f"Table {table_id} does not exist, creating new")

        # Create new table
        logger.info(f"Creating table {table_id} with {len(schema)} fields")
        table = bigquery.Table(table_id, schema=schema)
        table = bigquery_client.create_table(table)
        logger.info(f"Created table: {table_id}")

        # Load data
        if rows:
            success = load_data_to_bigquery(table_id, rows, schema)
            if not success:
                raise Exception("Failed to load data to BigQuery")

            result["rows_processed"] = len(rows)
            logger.info(f"Successfully processed {len(rows)} rows")

        result["processing_time"] = time.time() - start_time
        logger.info(f"Completed processing in {result['processing_time']:.2f} seconds")

    except Exception as e:
        error_msg = f"Error processing {blob_name}: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        log_error_to_bucket(error_msg)
        result["status"] = "failed"
        result["error"] = str(e)
        result["processing_time"] = time.time() - start_time

    return result


@app.route("/", methods=["GET", "POST"])
def process_geojson_files():
    """Main endpoint to process all GeoJSON files in the specified folder."""
    logger.info(f"Received {request.method} request to process GeoJSON files")

    results = {
        "processed_files": [],
        "failed_files": [],
        "total_files": 0,
        "start_time": datetime.utcnow().isoformat(),
        "project_id": PROJECT_ID,
        "bucket": BUCKET_NAME,
        "folder": FOLDER_PATH,
        "dataset": DATASET_ID,
    }

    try:
        # List all files in the specified folder
        logger.info(f"Listing files in gs://{BUCKET_NAME}/{FOLDER_PATH}")
        bucket = storage_client.bucket(BUCKET_NAME)

        # Use delimiter to get only files in the root of the folder
        blobs = bucket.list_blobs(prefix=f"{FOLDER_PATH}/", delimiter="/")

        # Collect GeoJSON files
        geojson_files = []
        for blob in blobs:

            if "reseau_souterrain_hta" not in blob.name:
                logger.info(f"Skipping non-position_geographique file: {blob.name}")
                continue
            if blob.name.endswith(".geojson"):
                geojson_files.append(blob.name)
                logger.info(f"Found GeoJSON file: {blob.name}")

        results["total_files"] = len(geojson_files)
        logger.info(f"Found {len(geojson_files)} GeoJSON files to process")

        if not geojson_files:
            logger.warning(
                f"No GeoJSON files found in gs://{BUCKET_NAME}/{FOLDER_PATH}/"
            )
            results["message"] = "No GeoJSON files found in the specified folder"
            return jsonify(results), 200

        # Process each file
        for idx, file_path in enumerate(geojson_files, 1):
            logger.info(f"Processing file {idx}/{len(geojson_files)}: {file_path}")
            process_result = process_geojson_file(BUCKET_NAME, file_path)

            if process_result["status"] == "success":
                results["processed_files"].append(process_result)
            else:
                results["failed_files"].append(process_result)

        results["end_time"] = datetime.utcnow().isoformat()

        # Calculate summary
        total_rows = sum(f.get("rows_processed", 0) for f in results["processed_files"])
        total_time = sum(
            f.get("processing_time", 0)
            for f in results["processed_files"] + results["failed_files"]
        )

        results["summary"] = {
            "total_rows_processed": total_rows,
            "total_processing_time": f"{total_time:.2f} seconds",
            "success_rate": f"{len(results['processed_files'])}/{results['total_files']}",
        }

        # Log summary
        summary_msg = (
            f"Processing completed: "
            f"{len(results['processed_files'])} succeeded, "
            f"{len(results['failed_files'])} failed, "
            f"{total_rows} total rows processed"
        )
        logger.info(summary_msg)

        # Log to bucket if there were any failures
        if results["failed_files"]:
            log_error_to_bucket(summary_msg)
            for failed in results["failed_files"]:
                log_error_to_bucket(f"Failed: {failed['file']} - {failed['error']}")

        return jsonify(results), 200

    except Exception as e:
        error_msg = (
            f"Fatal error in process_geojson_files: {str(e)}\n{traceback.format_exc()}"
        )
        logger.error(error_msg)
        log_error_to_bucket(error_msg)
        results["error"] = str(e)
        results["status"] = "error"
        return jsonify(results), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    try:
        # Test BigQuery connection
        bigquery_client.query("SELECT 1").result()

        # Test Storage connection
        bucket = storage_client.bucket(BUCKET_NAME)
        bucket.exists()

        return (
            jsonify(
                {
                    "status": "healthy",
                    "project_id": PROJECT_ID,
                    "bucket": BUCKET_NAME,
                    "dataset": DATASET_ID,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ),
            200,
        )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route("/test", methods=["GET"])
def test_endpoint():
    """Test endpoint to verify configuration."""
    try:
        # List first few files
        bucket = storage_client.bucket(BUCKET_NAME)
        blobs = list(
            bucket.list_blobs(prefix=f"{FOLDER_PATH}/", delimiter="/", max_results=5)
        )

        files = [blob.name for blob in blobs if blob.name.endswith(".geojson")]

        return (
            jsonify(
                {
                    "status": "ok",
                    "config": {
                        "project_id": PROJECT_ID,
                        "bucket": BUCKET_NAME,
                        "folder": FOLDER_PATH,
                        "dataset": DATASET_ID,
                    },
                    "sample_files": files,
                    "file_count": len(files),
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host="0.0.0.0", port=port)
