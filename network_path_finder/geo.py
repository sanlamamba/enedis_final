"""
Geospatial utilities for the Network Path Finder.
"""

import logging
import geopandas as gpd
from shapely.geometry import Point
from config import BT_LAYERS


def find_closest_bt_element(point, layers):
    """
    Find the closest BT network element to the given point.

    Args:
        point (Point): Shapely Point object (in EPSG:4326)
        layers (dict): Dictionary of layer_name -> GeoDataFrame

    Returns:
        tuple: (layer_name, feature_id, distance_km, feature) or None if no BT layers found
    """
    closest_feature = None
    closest_distance = float("inf")
    closest_layer = None

    # Project point to a metric CRS for accurate distance calculations
    point_gdf = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
    point_proj = point_gdf.to_crs("EPSG:3857")
    point_geom_proj = point_proj.geometry.iloc[0]

    for layer_name in BT_LAYERS:
        if layer_name not in layers:
            logging.warning(f"Layer {layer_name} not found")
            continue

        # Project to the same CRS for distance calculation
        gdf = layers[layer_name]
        gdf_proj = gdf.to_crs("EPSG:3857")

        distances = gdf_proj.geometry.distance(point_geom_proj)
        if distances.empty:
            continue

        min_idx = distances.idxmin()
        min_distance = distances[min_idx]

        if min_distance < closest_distance:
            closest_distance = min_distance
            closest_feature = gdf.iloc[min_idx]
            closest_layer = layer_name

    if closest_feature is None:
        return None

    feature_id = closest_feature["id"]
    distance_km = closest_distance / 1000.0

    logging.info(
        f"Closest BT element: {feature_id} ({closest_layer}) at {distance_km:.2f} km"
    )
    return (closest_layer, feature_id, distance_km, closest_feature)


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
