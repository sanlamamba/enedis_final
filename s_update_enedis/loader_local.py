"""Load electrical grid data from local folder."""

import json
import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from typing import Dict
import os
from config import (
    LAYERS,
    TARGET_CRS,
    LOCAL_DATA_PATH,
)

logger = logging.getLogger(__name__)


def load_csv_from_local(layer_key: str) -> gpd.GeoDataFrame:
    """Load CSV from local folder and convert to GeoDataFrame."""
    config = LAYERS[layer_key]

    # Construct the full path to the local file
    file_path = os.path.join(LOCAL_DATA_PATH, config.csv_file)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found locally: {file_path}")

    # Read CSV content
    df = pd.read_csv(file_path, delimiter=";", encoding="utf-8")

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


def load_all_layers_from_local() -> Dict[str, gpd.GeoDataFrame]:
    """Load all layers from local folder."""
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
