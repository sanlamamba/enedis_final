"""Load and process electrical grid data."""

import json
import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from typing import Dict

from config import DATA_DIR, LAYERS, TARGET_CRS

logger = logging.getLogger(__name__)


def load_csv_to_gdf(layer_key: str) -> gpd.GeoDataFrame:
    """Load CSV and convert to GeoDataFrame."""
    config = LAYERS[layer_key]
    csv_path = DATA_DIR / config.csv_file

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path, delimiter=";", encoding="utf-8")

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

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.to_crs(TARGET_CRS)

    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    gdf["layer"] = layer_key

    essential_cols = ["id", "layer", "geometry", "code_commune", "nom_commune"]
    cols_to_keep = [col for col in essential_cols if col in gdf.columns]

    return gdf[cols_to_keep]


def load_all_layers() -> Dict[str, gpd.GeoDataFrame]:
    """Load all layers from CSV files."""
    layers = {}

    for layer_key in LAYERS:
        try:
            logger.info(f"Loading layer: {layer_key}")
            gdf = load_csv_to_gdf(layer_key)
            layers[layer_key] = gdf
            logger.info(f"Loaded {len(gdf)} features for {layer_key}")
        except Exception as e:
            logger.error(f"Failed to load {layer_key}: {e}")
            raise

    return layers
