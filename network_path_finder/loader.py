"""
Data loading utilities for the Network Path Finder.
"""

import logging
import geopandas as gpd
from pathlib import Path
from .config import ALL_LAYERS


def load_geojson_layers(data_dir):
    """
    Load all GeoJSON layers from the specified directory.

    Args:
        data_dir (Path): Path to directory containing GeoJSON files

    Returns:
        dict: Dictionary of layer_name -> GeoDataFrame
    """
    layers = {}

    for layer_name in ALL_LAYERS:
        file_path = data_dir / f"{layer_name}.geojson"
        if file_path.exists():
            logging.info(f"Loading layer: {layer_name}")
            gdf = gpd.read_file(file_path)

            # Ensure CRS is set to WGS84 for consistent operations
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326")
            elif gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")

            layers[layer_name] = gdf
        else:
            logging.warning(f"File not found: {file_path}")

    return layers


def build_network_lookup(layers):
    """
    Build a lookup dictionary of all features across all layers.

    Args:
        layers (dict): Dictionary of layer_name -> GeoDataFrame

    Returns:
        dict: Dictionary of feature_id -> feature information
    """
    all_features = {}

    for layer_name, gdf in layers.items():
        for idx, row in gdf.iterrows():
            feature_id = row["id"]
            all_features[feature_id] = {
                "layer": layer_name,
                "connections": row.get("connections", []),
                "feature": row,
            }

    return all_features
