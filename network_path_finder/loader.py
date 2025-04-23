"""
Data loading utilities for the Network Path Finder.
"""

import logging
import geopandas as gpd
from pathlib import Path
from shapely.geometry import shape
from config import ALL_LAYERS
import json


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

            try:
                with open(file_path, "r") as f:
                    geojson_data = json.load(f)

                features = []
                for feature in geojson_data.get("features", []):
                    properties = feature.get("properties", {})
                    geometry_obj = feature.get("geometry", {})

                    feature_dict = properties.copy()

                    feature_dict["geometry"] = shape(geometry_obj)

                    for conn_field in [
                        "connections",
                        "start_connections",
                        "end_connections",
                    ]:
                        if conn_field in properties:
                            if isinstance(properties[conn_field], list):
                                feature_dict[conn_field] = properties[conn_field]
                            elif properties[conn_field] is None:
                                feature_dict[conn_field] = []
                        else:
                            feature_dict[conn_field] = []

                    features.append(feature_dict)

                gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")

                if gdf.crs is None:
                    gdf = gdf.set_crs("EPSG:4326")
                elif gdf.crs != "EPSG:4326":
                    gdf = gdf.to_crs("EPSG:4326")

                gdf["source_layer"] = layer_name

                layers[layer_name] = gdf

            except Exception as e:
                logging.error(f"Error loading {file_path}: {e}")
        else:
            logging.warning(f"File not found: {file_path}")

    return layers


def build_network_lookup(layers):
    """
    Build a lookup dictionary of all features across all layers.
    """
    all_features = {}

    for layer_name, gdf in layers.items():
        for idx, row in gdf.iterrows():
            feature_id = row["id"]

            connections = row.get("connections", [])
            if isinstance(connections, list) and len(connections) == 1:
                connections = connections[0]

            all_features[feature_id] = {
                "layer": layer_name,
                "connections": connections,
                "feature": row,
            }

    return all_features
