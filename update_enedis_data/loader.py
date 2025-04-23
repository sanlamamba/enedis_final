#!/usr/bin/env python3
"""
Data loading module for the Enedis project.

This module provides functions for loading data from CSV or GeoJSON files.
CSV data is converted to GeoDataFrame by extracting geometry from the 'geo_shape' field.
Each layer is defined in LAYERS_CONFIG.
"""

import os
import logging
import json
from typing import Dict, Optional, Union
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from config import (
    DATA_DIR,
    PROCESSED_DIR,
    LAYERS_CONFIG,
    TARGET_CRS,
    USE_CLOUD_STORAGE,
    CLOUD_BUCKET_NAME,
    CLOUD_DATA_DIR,
)
from cloud_storage_utils import read_text_from_cloud, download_blob_to_temp_file
from utils import extract_geometry_from_json, retry, timed


@timed
def load_csv_to_gdf(layer_key: str) -> gpd.GeoDataFrame:
    """
    Load a CSV file for a given layer and convert it to a GeoDataFrame.

    If cloud storage is enabled, the file is retrieved from the cloud bucket.
    In case of error, the exception is raised.

    Args:
        layer_key: Key identifying the layer in LAYERS_CONFIG.

    Returns:
        GeoDataFrame with the data from the CSV.

    Raises:
        FileNotFoundError: If the CSV file is not found.
        ValueError: If no valid geometries are found in the CSV.
    """
    config = LAYERS_CONFIG[layer_key]
    csv_filename = config.csv_file

    try:
        if USE_CLOUD_STORAGE:
            logging.info(
                f"Loading CSV for layer '{layer_key}' from cloud bucket '{CLOUD_BUCKET_NAME}'..."
            )
            cloud_csv_path = f"{CLOUD_DATA_DIR}/{csv_filename}"
            temp_file_path = download_blob_to_temp_file(
                CLOUD_BUCKET_NAME, cloud_csv_path
            )
            df = pd.read_csv(
                temp_file_path,
                delimiter=";",
                encoding="utf-8",
                # engine="python",
                on_bad_lines="warn",
                # low_memory=False,
            )
            os.remove(temp_file_path)
        else:
            file_path = os.path.join(DATA_DIR, csv_filename)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            logging.info(f"Loading CSV for layer '{layer_key}' from {file_path}...")
            df = pd.read_csv(
                file_path,
                delimiter=";",
                encoding="utf-8",
                # engine="python",
                on_bad_lines="warn",
                # low_memory=False,
            )
    except Exception as e:
        logging.error(f"Failed to load CSV for layer '{layer_key}': {e}")
        raise

    batch_size = 10000
    total_rows = len(df)
    geometries = []

    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i : i + batch_size]
        batch_geometries = [
            extract_geometry_from_json(geojson_str)
            for geojson_str in batch["geo_shape"]
        ]
        geometries.extend(batch_geometries)

    df["geometry"] = geometries
    df = df[df["geometry"].notnull()].copy()

    if df.empty:
        raise ValueError(f"No valid geometries found in CSV for layer '{layer_key}'")

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").to_crs(TARGET_CRS)
    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    return gdf


@timed
def load_geojson(layer_key: str) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON file for a given layer and return a GeoDataFrame.

    Args:
        layer_key: Key identifying the layer in LAYERS_CONFIG.

    Returns:
        GeoDataFrame with the data from the GeoJSON.

    Raises:
        FileNotFoundError: If the GeoJSON file is not found.
    """
    config = LAYERS_CONFIG[layer_key]
    file_path = os.path.join(PROCESSED_DIR, config.geojson_file)

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"GeoJSON not found: {file_path}")

        logging.info(f"Loading GeoJSON for layer '{layer_key}' from {file_path}...")
        gdf = gpd.read_file(file_path)
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf["source_layer"] = layer_key
        return gdf

    except Exception as e:
        logging.error(f"Failed to load GeoJSON for layer '{layer_key}': {e}")
        raise


@timed
def load_all_layers(use_csv: bool = False) -> Dict[str, gpd.GeoDataFrame]:
    """
    Load all layers defined in LAYERS_CONFIG.
    Stops immediately if a layer fails to load.

    Args:
        use_csv: If True, layers are loaded from CSV, otherwise from GeoJSON.

    Returns:
        Dictionary with layer names as keys and GeoDataFrames as values.
    """
    layers = {}

    from concurrent.futures import ThreadPoolExecutor

    def load_layer(layer_key):
        if use_csv:
            return layer_key, load_csv_to_gdf(layer_key)
        else:
            return layer_key, load_geojson(layer_key)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(load_layer, layer_key) for layer_key in LAYERS_CONFIG
        ]
        for future in futures:
            try:
                layer_key, gdf = future.result()
                layers[layer_key] = gdf
            except Exception as e:
                logging.error(f"Error loading layer: {e}")
                raise

    return layers


@retry(max_attempts=3)
def load_cached_layer(layer_key: str, use_csv: bool = False) -> gpd.GeoDataFrame:
    """
    Load a layer with caching capabilities.

    Args:
        layer_key: Key identifying the layer in LAYERS_CONFIG.
        use_csv: If True, load from CSV, otherwise from GeoJSON.

    Returns:
        GeoDataFrame for the specified layer.
    """
    cache_dir = PROCESSED_DIR / "cache"
    os.makedirs(cache_dir, exist_ok=True)

    cache_file = cache_dir / f"{layer_key}_cached.gpkg"

    if (
        cache_file.exists()
        and (
            pd.Timestamp.now() - pd.Timestamp.fromtimestamp(cache_file.stat().st_mtime)
        ).days
        < 1
    ):
        try:
            logging.info(f"Loading cached layer '{layer_key}' from {cache_file}")
            return gpd.read_file(cache_file)
        except Exception as e:
            logging.warning(f"Failed to load cached layer, will reload: {e}")

    if use_csv:
        gdf = load_csv_to_gdf(layer_key)
    else:
        gdf = load_geojson(layer_key)

    try:
        gdf.to_file(cache_file, driver="GPKG")
        logging.info(f"Saved layer '{layer_key}' to cache at {cache_file}")
    except Exception as e:
        logging.warning(f"Failed to cache layer: {e}")

    return gdf
