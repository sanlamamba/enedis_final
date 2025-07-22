"""Load electrical grid data from Google Cloud Storage or local files."""

import json
import logging
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from typing import Dict
from google.cloud import storage
import io

from config import (
    LAYERS,
    TARGET_CRS,
    CLOUD_BUCKET_NAME,
    CLOUD_DATA_PATH,
    GCP_PROJECT_ID,
)

# Local data path
LOCAL_DATA_PATH = "../data"

logger = logging.getLogger(__name__)


def load_csv_from_cloud(layer_key: str) -> gpd.GeoDataFrame:
    """Load CSV from Google Cloud Storage and convert to GeoDataFrame."""
    config = LAYERS[layer_key]

    # Initialize GCS client
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(CLOUD_BUCKET_NAME)

    # Construct the full path in the bucket
    blob_path = f"{CLOUD_DATA_PATH}/{config.csv_file}"
    blob = bucket.blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"CSV file not found in cloud: {blob_path}")

    # Download CSV content
    csv_content = blob.download_as_text(encoding="utf-8")
    df = pd.read_csv(io.StringIO(csv_content), delimiter=";", encoding="utf-8")

    # Process geometries
    geometries = []
    for geojson_str in df["geo_shape"]:
        try:
            geo_dict = json.loads(geojson_str)
            geometries.append(shape(geo_dict))
        except:
            geometries.append(None)

    df["geometry"] = geometries
    df = df[df["geometry"].notnull()].copy()

    if df.empty:
        raise ValueError(f"No valid geometries found for layer '{layer_key}'")

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.to_crs(TARGET_CRS)

    # Add essential columns
    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    gdf["layer"] = layer_key

    # Keep only essential columns
    essential_cols = ["id", "layer", "geometry", "code_commune", "nom_commune"]
    cols_to_keep = [col for col in essential_cols if col in gdf.columns]

    return gdf[cols_to_keep]


def load_csv_from_local(layer_key: str) -> gpd.GeoDataFrame:
    """Load CSV from local data folder and convert to GeoDataFrame."""
    config = LAYERS[layer_key]

    # Construct the full local path
    csv_path = f"{LOCAL_DATA_PATH}/{config.csv_file}"

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found locally: {csv_path}")

    # Load CSV file
    df = pd.read_csv(csv_path, delimiter=";", encoding="utf-8")

    # Process geometries
    geometries = []
    for geojson_str in df["geo_shape"]:
        try:
            geo_dict = json.loads(geojson_str)
            geometries.append(shape(geo_dict))
        except:
            geometries.append(None)

    df["geometry"] = geometries
    df = df[df["geometry"].notnull()].copy()

    if df.empty:
        raise ValueError(f"No valid geometries found for layer '{layer_key}'")

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    # keep the original CRS
    gdf["original_coordinates"] = gdf.geometry.apply(
        lambda geom: geom.coords[:] if geom else None
    )

    # Add essential columns
    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    gdf["layer"] = layer_key
    gdf = gdf.to_crs(TARGET_CRS)

    # Keep only essential columns
    essential_cols = [
        "id",
        "layer",
        "geometry",
        "code_commune",
        "nom_commune",
        "original_coordinates",
    ]
    cols_to_keep = [col for col in essential_cols if col in gdf.columns]

    return gdf[cols_to_keep]


def load_all_layers_from_cloud() -> Dict[str, gpd.GeoDataFrame]:
    """Load all layers from Google Cloud Storage."""
    layers = {}

    for layer_key in LAYERS:
        try:
            logger.info(f"Loading layer from cloud: {layer_key}")
            gdf = load_csv_from_cloud(layer_key)
            layers[layer_key] = gdf
            logger.info(f"Loaded {len(gdf)} features for {layer_key}")
        except Exception as e:
            logger.error(f"Failed to load {layer_key} from cloud: {e}")
            raise

    return layers


def load_all_layers_from_local() -> Dict[str, gpd.GeoDataFrame]:
    """Load all layers from local data folder."""
    layers = {}

    for layer_key in LAYERS:
        try:
            logger.info(f"Loading layer from local: {layer_key}")
            gdf = load_csv_from_local(layer_key)
            layers[layer_key] = gdf
            logger.info(f"Loaded {len(gdf)} features for {layer_key}")
        except Exception as e:
            logger.error(f"Failed to load {layer_key} from local: {e}")
            raise

    return layers


def load_all_layers(source: str = "local") -> Dict[str, gpd.GeoDataFrame]:
    """Load all layers from specified source (local or cloud).

    Args:
        source: Either 'local' or 'cloud' to specify data source

    Returns:
        Dictionary mapping layer keys to GeoDataFrames
    """
    if source.lower() == "cloud":
        return load_all_layers_from_cloud()
    elif source.lower() == "local":
        return load_all_layers_from_local()
    else:
        raise ValueError("Source must be either 'local' or 'cloud'")


def load_layer(layer_key: str, source: str = "local") -> gpd.GeoDataFrame:
    """Load a single layer from specified source (local or cloud).

    Args:
        layer_key: Key identifying the layer to load
        source: Either 'local' or 'cloud' to specify data source

    Returns:
        GeoDataFrame containing the layer data
    """
    if source.lower() == "cloud":
        return load_csv_from_cloud(layer_key)
    elif source.lower() == "local":
        return load_csv_from_local(layer_key)
    else:
        raise ValueError("Source must be either 'local' or 'cloud'")
