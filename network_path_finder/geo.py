"""
Geospatial utilities for the Network Path Finder.
"""

import logging
import geopandas as gpd
from shapely.geometry import Point
from config import BT_LAYERS, MAX_BT_DISTANCE


def find_closest_bt_elements(point, layers, max_elements=10):
    """
    Find multiple BT network elements closest to the given point, sorted by distance.

    Args:
        point (Point): Shapely Point object (in EPSG:4326)
        layers (dict): Dictionary of layer_name -> GeoDataFrame
        max_elements (int): Maximum number of closest elements to return

    Returns:
        list: List of tuples (layer_name, feature_id, distance_km, feature) sorted by distance
    """
    point_gdf = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
    point_proj = point_gdf.to_crs("EPSG:3857")
    point_geom_proj = point_proj.geometry.iloc[0]

    all_distances = []

    for layer_name in BT_LAYERS:
        if layer_name not in layers:
            logging.warning(f"Layer {layer_name} not found")
            continue

        gdf = layers[layer_name]
        if gdf.empty:
            continue

        gdf_proj = gdf.to_crs("EPSG:3857")

        distances = gdf_proj.geometry.distance(point_geom_proj)
        if distances.empty:
            continue

        for idx, distance in distances.items():
            feature = gdf.iloc[idx]
            feature_id = feature["id"]
            distance_km = distance / 1000.0

            if distance_km <= MAX_BT_DISTANCE:
                all_distances.append((distance_km, layer_name, feature_id, feature))

    all_distances.sort()
    closest_elements = []

    for i, (distance_km, layer_name, feature_id, feature) in enumerate(
        all_distances[:max_elements]
    ):
        logging.info(
            f"BT element #{i+1}: {feature_id} ({layer_name}) at {distance_km:.2f} km"
        )
        closest_elements.append((layer_name, feature_id, distance_km, feature))

    return closest_elements


def extract_path_features(path, layers):
    """
    Extract full feature information for each element in the path.

    Args:
        path (list): List of (layer, id) tuples
        layers (dict): Dictionary of layer_name -> GeoDataFrame

    Returns:
        list: List of GeoJSON features
    """
    import json

    path_features = []

    for layer_name, feature_id in path:
        gdf = layers[layer_name]
        feature = gdf[gdf["id"] == feature_id]

        if not feature.empty:
            # Convert to GeoJSON feature
            geojson_feature = json.loads(feature.to_json())["features"][0]
            path_features.append(geojson_feature)

    return path_features
