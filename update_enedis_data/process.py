#!/usr/bin/env python3
"""
CSV data processing module and spatial connection calculation.

This module loads CSV data for each layer, calculates spatial connections
between entities, and saves the result as GeoJSON files.
"""

import os
import logging
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import geopandas as gpd
import pandas as pd

from config import (
    PROCESSED_DIR,
    LAYERS_CONFIG,
    USE_CLOUD_STORAGE,
    CLOUD_BUCKET_NAME,
    SHOULD_EXPORT_GEOJSON,
    SHOULD_EXPORT_SHAPEFILE,
)
from loader import load_csv_to_gdf
from cloud_storage_utils import upload_file_to_cloud
from utils import add_filedate, timed, create_temp_file
from chunked_processor import process_layers_in_chunks


@timed
def save_updated_layers(updated_layers: Dict[str, gpd.GeoDataFrame]) -> List[str]:
    """
    Save each updated GeoDataFrame as GeoJSON.
    If cloud storage is enabled (USE_CLOUD_STORAGE), the file is also uploaded to the cloud bucket.
    Local saving is done in the PROCESSED_DIR directory.

    Args:
        updated_layers: Dictionary of updated GeoDataFrames.

    Returns:
        List of paths to saved files
    """
    if not updated_layers:
        logging.info("No layers to save.")
        return []

    saved_files = []

    with ThreadPoolExecutor() as executor:
        futures = []

        for layer_key, gdf in updated_layers.items():
            if SHOULD_EXPORT_GEOJSON:
                futures.append(executor.submit(save_geojson, gdf, layer_key))
            if SHOULD_EXPORT_SHAPEFILE:
                futures.append(executor.submit(save_shapefile, gdf, layer_key))

        for future in futures:
            try:
                result = future.result()
                if result:
                    saved_files.append(result)
            except Exception as e:
                logging.error(f"Error saving layer file: {e}")

    return saved_files


@timed
def save_geojson(gdf: gpd.GeoDataFrame, layer_key: str) -> Optional[str]:
    """
    Save a GeoDataFrame as a GeoJSON file for a given layer.

    Args:
        gdf: Dataset to save.
        layer_key: Layer key in LAYERS_CONFIG to determine the filename.

    Returns:
        Path to saved file or None if an error occurred
    """
    gdf = add_filedate(gdf)
    gdf = gdf.to_crs("EPSG:4326")
    config = LAYERS_CONFIG.get(layer_key)
    if not config:
        logging.error(f"No configuration found for layer {layer_key}")
        return None

    output_filename = config.geojson_file

    try:
        if USE_CLOUD_STORAGE:
            logging.info(
                f"Saving GeoJSON for layer '{layer_key}' to Cloud Storage in bucket {CLOUD_BUCKET_NAME}..."
            )
            temp_path = create_temp_file(suffix=".geojson")

            gdf.to_file(temp_path, driver="GeoJSON", encoding="utf-8")

            cloud_path = upload_file_to_cloud(
                temp_path,
                CLOUD_BUCKET_NAME,
                output_filename,
                content_type="application/geo+json",
            )

            os.remove(temp_path)

            logging.info(
                f"Saved GeoJSON for layer '{layer_key}' to cloud as {output_filename}."
            )
            return cloud_path
        else:
            os.makedirs(PROCESSED_DIR, exist_ok=True)
            output_path = os.path.join(PROCESSED_DIR, output_filename)

            gdf.to_file(output_path, driver="GeoJSON", encoding="utf-8")

            logging.info(
                f"Saved updated GeoJSON for layer '{layer_key}' to {output_path}"
            )
            return output_path
    except Exception as e:
        logging.error(f"Error saving GeoJSON for {layer_key}: {e}")
        return None


@timed
def save_shapefile(gdf: gpd.GeoDataFrame, layer_key: str) -> Optional[str]:
    """
    Save a GeoDataFrame as a ShapeFile for a given layer.

    Args:
        gdf: Dataset to save.
        layer_key: Layer key in LAYERS_CONFIG used to determine the filename.

    Returns:
        Path to saved file or None if an error occurred
    """
    gdf = add_filedate(gdf)

    try:
        if USE_CLOUD_STORAGE:
            logging.info(
                f"Saving Shapefile for layer '{layer_key}' to Cloud Storage in bucket {CLOUD_BUCKET_NAME}..."
            )

            with tempfile.TemporaryDirectory() as tmpdirname:
                base_filename = layer_key
                shapefile_path = os.path.join(tmpdirname, base_filename + ".shp")

                gdf.to_file(shapefile_path, driver="ESRI Shapefile")

                config = LAYERS_CONFIG.get(layer_key, {})
                zip_filename = getattr(config, "shapefile_file", f"{layer_key}.zip")
                zip_path = os.path.join(tmpdirname, zip_filename)

                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for filename in os.listdir(tmpdirname):
                        if filename.startswith(base_filename):
                            full_path = os.path.join(tmpdirname, filename)
                            zipf.write(full_path, arcname=filename)

                cloud_path = upload_file_to_cloud(
                    zip_path,
                    CLOUD_BUCKET_NAME,
                    zip_filename,
                    content_type="application/zip",
                )

                logging.info(
                    f"Saved Shapefile for layer '{layer_key}' to cloud as {zip_filename}."
                )
                return cloud_path
        else:
            os.makedirs(PROCESSED_DIR, exist_ok=True)
            shapefile_path = os.path.join(PROCESSED_DIR, layer_key + ".shp")

            gdf.to_file(shapefile_path, driver="ESRI Shapefile")

            logging.info(f"Saved Shapefile for layer '{layer_key}' to {shapefile_path}")
            return shapefile_path
    except Exception as e:
        logging.error(f"Error saving shapefile for {layer_key}: {e}")
        return None


def process_csv_layers_with_chunking() -> Dict[str, gpd.GeoDataFrame]:
    """
    Process CSV data in chunks to reduce memory usage.
    """
    layers = {}

    for layer_key in LAYERS_CONFIG:
        try:
            logging.info(f"Loading layer '{layer_key}' from CSV...")
            gdf = load_csv_to_gdf(layer_key)

            if "source_layer" not in gdf.columns:
                gdf["source_layer"] = layer_key

            output_filename = LAYERS_CONFIG[layer_key].geojson_file
            output_path = os.path.join(PROCESSED_DIR, output_filename)
            gdf.to_file(output_path, driver="GeoJSON")

            simplified = gdf[["id", "geometry", "source_layer"]].copy()
            layers[layer_key] = simplified

        except Exception as e:
            logging.error(f"Error processing layer '{layer_key}': {e}")
            raise

    processed_layers = {}
    for layer_key in LAYERS_CONFIG:
        try:
            logging.info(f"Processing layer '{layer_key}' in chunks...")
            output_filename = LAYERS_CONFIG[layer_key].geojson_file
            file_path = os.path.join(PROCESSED_DIR, output_filename)

            processed_gdf = process_layers_in_chunks(
                layer_key,
                file_path,
                layers,
                chunk_size=10000,  # 2000 rows for 4gb RAM, 5000 for 8gb RAM, 10000 for 16gb RAM, 50000 for 32gb RAM
                overlap_buffer=LAYERS_CONFIG[layer_key].radius * 2,
            )

            processed_layers[layer_key] = processed_gdf

        except Exception as e:
            logging.error(f"Error processing layer '{layer_key}' in chunks: {e}")
            raise

    return processed_layers


@timed
def process_csv_layers() -> Dict[str, gpd.GeoDataFrame]:
    """
    Load CSV data for each layer, convert to GeoDataFrame,
    save each layer as GeoJSON and return the dictionary of GeoDataFrames.

    Returns:
        Dictionary associating each layer key with its corresponding GeoDataFrame.
    """
    layers = {}

    with ThreadPoolExecutor() as executor:
        future_to_layer = {
            executor.submit(load_csv_to_gdf, layer_key): layer_key
            for layer_key in LAYERS_CONFIG
        }

        for future in future_to_layer:
            layer_key = future_to_layer[future]
            try:
                logging.info(f"Processing layer '{layer_key}' from CSV...")
                gdf = future.result()
                layers[layer_key] = gdf

                if SHOULD_EXPORT_GEOJSON:
                    save_geojson(gdf, layer_key)
                if SHOULD_EXPORT_SHAPEFILE:
                    save_shapefile(gdf, layer_key)

            except Exception as e:
                logging.error(f"Error processing layer '{layer_key}': {e}")
                raise

    return layers


def validate_layer_data(layers: Dict[str, gpd.GeoDataFrame]) -> Dict[str, List[str]]:
    """
    Validate the data in each layer for common issues.

    Args:
        layers: Dictionary of layer GeoDataFrames

    Returns:
        Dictionary of issues found per layer
    """
    validation_results = {}

    for layer_key, gdf in layers.items():
        issues = []

        if gdf.geometry.isna().any():
            count = gdf.geometry.isna().sum()
            issues.append(f"Found {count} rows with missing geometries")

        invalid_geoms = ~gdf.geometry.is_valid
        if invalid_geoms.any():
            count = invalid_geoms.sum()
            issues.append(f"Found {count} rows with invalid geometries")

        if gdf["id"].duplicated().any():
            count = gdf["id"].duplicated().sum()
            issues.append(f"Found {count} duplicate IDs")

        required_cols = ["id", "geometry"]
        for col in required_cols:
            if col not in gdf.columns:
                issues.append(f"Missing required column: {col}")

        validation_results[layer_key] = issues

        if issues:
            logging.warning(
                f"Validation issues for layer '{layer_key}':\n" + "\n".join(issues)
            )
        else:
            logging.info(f"Layer '{layer_key}' passed validation")

    return validation_results
