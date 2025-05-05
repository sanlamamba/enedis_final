#!/usr/bin/env python3
"""
BigQuery upload module for the Enedis project.

Each GeoJSON is loaded into a separate BigQuery table named after the file,
in the defined dataset.
"""

import os
import json
import logging
import tempfile
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import geopandas as gpd
from shapely.geometry import shape

from config import PROCESSED_DIR, LAYERS_CONFIG, BIGQUERY_TABLE_ID
from utils import timed, retry


def extract_record(feature: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a structured record from a GeoJSON feature.

    Args:
        feature: GeoJSON feature dictionary

    Returns:
        Structured record for BigQuery insertion

    Raises:
        ValueError: If feature doesn't have required properties
    """
    props = feature.get("properties", {})
    geom_obj = feature.get("geometry", {})

    if not props or not geom_obj:
        raise ValueError("Feature missing required properties or geometry")

    node_ref = props.get("id")
    if not node_ref:
        raise ValueError("Feature missing ID property")

    try:
        record_id = int(node_ref.split("_")[-1])
    except Exception as e:
        logging.error(f"Error extracting integer ID from {node_ref}: {e}")
        raise

    try:
        from shapely.geometry import shape

        geom_wkt = shape(geom_obj).wkt
    except Exception as e:
        logging.error(f"Error converting geometry: {e}")
        raise

    # Format coordinates as string
    coordinates = (
        ",".join(str(c) for c in geom_obj.get("coordinates", []))
        if geom_obj.get("coordinates")
        else ""
    )
    geom_type = geom_obj.get("type", "Unknown")

    # Extract and return record with all required fields
    return {
        "id": record_id,
        "node_ref": node_ref,
        "code_commune": props.get("code_commune"),
        "nom_commune": props.get("nom_commune"),
        "code_epci": props.get("code_epci"),
        "nom_epci": props.get("nom_epci"),
        "code_departement": props.get("code_departement"),
        "nom_departement": props.get("nom_departement"),
        "code_region": props.get("code_region"),
        "nom_region": props.get("nom_region"),
        "connections": props.get("connections", []),
        "start_connections": props.get("start_connections", []),
        "end_connections": props.get("end_connections", []),
        "geometry": geom_wkt,
        "coordinates": coordinates,
        "type": geom_type,
    }


@timed
def upload_layers_to_bigquery(
    updated_layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, int]:
    """
    Upload each updated GeoJSON layer to its dedicated BigQuery table.

    Args:
        updated_layers: Dictionary of updated GeoDataFrames

    Returns:
        Dictionary mapping layer keys to row counts inserted
    """
    if not updated_layers:
        logging.warning("No layers to upload to BigQuery")
        return {}

    results = {}

    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=min(4, len(updated_layers))) as executor:
        future_to_layer = {
            executor.submit(
                upload_layer_to_bigquery, layer_key, updated_layers
            ): layer_key
            for layer_key in updated_layers
        }

        for future in future_to_layer:
            layer_key = future_to_layer[future]
            try:
                row_count = future.result()
                if row_count is not None:
                    results[layer_key] = row_count
                    logging.info(
                        f"Successfully uploaded {row_count} rows for layer '{layer_key}'"
                    )
                else:
                    logging.warning(f"No data uploaded for layer '{layer_key}'")
            except Exception as e:
                logging.error(f"Failed to upload layer '{layer_key}' to BigQuery: {e}")

    return results


@retry(max_attempts=3, exceptions=(GoogleCloudError, ValueError, IOError))
@timed
def upload_layer_to_bigquery(
    layer_key: str, updated_layers: Dict[str, gpd.GeoDataFrame]
) -> Optional[int]:
    """
    Upload a specific layer to BigQuery.

    Args:
        layer_key: Key of the layer to upload
        updated_layers: Dictionary of updated GeoDataFrames

    Returns:
        Number of rows inserted or None if failed
    """
    if layer_key not in updated_layers:
        logging.warning(f"Layer {layer_key} not found in updated layers")
        return None

    client = bigquery.Client()

    # Parse table components
    table_components = BIGQUERY_TABLE_ID.split(".")
    if len(table_components) != 2:
        raise ValueError(f"Invalid BigQuery table ID format: {BIGQUERY_TABLE_ID}")

    project, dataset = table_components

    output_filename = LAYERS_CONFIG[layer_key].geojson_file
    table_name = output_filename.replace(".geojson", "")
    full_table_id = f"{project}.{dataset}.{table_name}"

    # Check if we have the GeoDataFrame already
    gdf = updated_layers[layer_key]
    if gdf is not None and not gdf.empty:
        # Direct conversion from GeoDataFrame to records
        logging.info(
            f"Converting GeoDataFrame for layer '{layer_key}' to records for BigQuery..."
        )

        # Process in batches to avoid memory issues
        batch_size = 1000
        total_rows = 0

        # Process in batches
        for i in range(0, len(gdf), batch_size):
            batch_gdf = gdf.iloc[i : i + batch_size]
            records = []

            for _, row in batch_gdf.iterrows():
                try:
                    # Create a GeoJSON-like feature from the row
                    feature = {
                        "type": "Feature",
                        "geometry": json.loads(row.geometry.to_json()),
                        "properties": {k: v for k, v in row.items() if k != "geometry"},
                    }
                    records.append(extract_record(feature))
                except Exception as e:
                    logging.error(f"Error creating record from row: {e}")
                    continue

            if records:
                rows_inserted = _write_records_to_bigquery(
                    records, full_table_id, client, i == 0
                )
                total_rows += rows_inserted

        return total_rows

    # Fall back to reading from file if GeoDataFrame approach doesn't work
    file_path = os.path.join(PROCESSED_DIR, output_filename)
    logging.info(f"Reading GeoJSON '{file_path}' for upload to '{full_table_id}'...")

    try:
        with open(file_path, "r") as f:
            geojson = json.load(f)
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        raise

    if "features" not in geojson:
        raise ValueError(f"No features found in {file_path}")

    # Process records in batches for better performance
    records = []
    batch_size = 1000
    total_rows = 0

    for i, feature in enumerate(geojson["features"]):
        try:
            records.append(extract_record(feature))

            # Process in batches to avoid memory issues with large files
            if len(records) >= batch_size:
                rows_inserted = _write_records_to_bigquery(
                    records, full_table_id, client, i < batch_size
                )
                total_rows += rows_inserted
                records = []

        except Exception as e:
            logging.error(f"Error extracting record at index {i}: {e}")
            continue

    # Process any remaining records
    if records:
        rows_inserted = _write_records_to_bigquery(
            records, full_table_id, client, total_rows == 0
        )
        total_rows += rows_inserted

    return total_rows


def _write_records_to_bigquery(
    records: List[Dict[str, Any]],
    full_table_id: str,
    client: bigquery.Client,
    is_first_batch: bool,
) -> int:
    """
    Write a batch of records to BigQuery.

    Args:
        records: List of record dictionaries to write
        full_table_id: Full BigQuery table ID (project.dataset.table)
        client: BigQuery client instance
        is_first_batch: If True, use WRITE_TRUNCATE, otherwise WRITE_APPEND

    Returns:
        Number of rows inserted
    """
    if not records:
        logging.warning("No records to write to BigQuery")
        return 0

    logging.info(f"Writing {len(records)} records to {full_table_id}")

    # Write to newline-delimited JSON
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as tmp_file:
        for rec in records:
            tmp_file.write(json.dumps(rec) + "\n")
        temp_filename = tmp_file.name

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if is_first_batch
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("node_ref", "STRING"),
            bigquery.SchemaField("code_commune", "INTEGER"),
            bigquery.SchemaField("nom_commune", "STRING"),
            bigquery.SchemaField("code_epci", "INTEGER"),
            bigquery.SchemaField("nom_epci", "STRING"),
            bigquery.SchemaField("code_departement", "INTEGER"),
            bigquery.SchemaField("nom_departement", "STRING"),
            bigquery.SchemaField("code_region", "INTEGER"),
            bigquery.SchemaField("nom_region", "STRING"),
            bigquery.SchemaField("connections", "STRING", mode="REPEATED"),
            bigquery.SchemaField("start_connections", "STRING", mode="REPEATED"),
            bigquery.SchemaField("end_connections", "STRING", mode="REPEATED"),
            bigquery.SchemaField("geometry", "GEOGRAPHY"),
            bigquery.SchemaField("coordinates", "STRING"),
            bigquery.SchemaField("type", "STRING"),
        ],
    )

    try:
        with open(temp_filename, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, full_table_id, job_config=job_config
            )
        result = load_job.result()  # Wait for job to complete
        logging.info(f"{load_job.output_rows} rows inserted into {full_table_id}")
        return load_job.output_rows
    except Exception as e:
        logging.error(f"Error loading data to {full_table_id}: {e}")
        raise
    finally:
        os.remove(temp_filename)
