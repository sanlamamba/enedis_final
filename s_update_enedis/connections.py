"""Optimized connections calculation for electrical grid components."""

import logging
from typing import Dict, List
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

from config import LAYERS
from exporter_local import save_to_local_storage

logger = logging.getLogger(__name__)


def get_endpoint_connections(
    endpoint: Point,
    candidates: gpd.GeoDataFrame,
    layer_config,
    feature_layer: str,
    max_connections: int = 10,
) -> List[str]:
    """Find connections for a single endpoint based on electrical grid hierarchy."""
    if candidates.empty:
        return []

    # Calculate distances and filter by connection radius
    distances = candidates.geometry.distance(endpoint)
    candidates = candidates.copy()
    candidates["distance"] = distances
    candidates = candidates[candidates["distance"] <= layer_config.connection_radius]

    if candidates.empty:
        return []

    # Filter by allowed connection types
    allowed_layers = set(layer_config.can_connect_to)
    candidates = candidates[candidates["layer"].isin(allowed_layers)]

    if candidates.empty:
        return []

    # Sort by priority and distance
    candidates["priority"] = candidates["layer"].map(
        lambda x: LAYERS[x].priority if x in LAYERS else 999
    )
    candidates = candidates.sort_values(["priority", "distance"])

    # Find valid connections
    valid_connections = []
    for _, candidate in candidates.iterrows():
        candidate_layer = candidate["layer"]
        candidate_config = LAYERS.get(candidate_layer)

        if candidate_config and feature_layer in candidate_config.can_connect_to:
            valid_connections.append(candidate["id"])

            if len(valid_connections) >= max_connections:
                break

    return valid_connections


def calculate_layer_connections(
    gdf: gpd.GeoDataFrame, all_features: gpd.GeoDataFrame, layer_key: str
) -> gpd.GeoDataFrame:
    """Calculate connections for all features in a layer."""
    layer_config = LAYERS[layer_key]

    # Project to metric CRS for distance calculations
    gdf_proj = gdf.to_crs("EPSG:3857")
    all_features_proj = all_features.to_crs("EPSG:3857")

    # Create spatial index for efficient queries
    spatial_index = all_features_proj.sindex

    connections_list = []

    for idx, feature in gdf_proj.iterrows():
        geom = feature.geometry
        feature_id = feature["id"]

        # Define search radius
        search_radius = layer_config.connection_radius * 1.5

        if geom.geom_type == "LineString":
            # For lines, check both endpoints
            start_point = Point(geom.coords[0])
            end_point = Point(geom.coords[-1])

            # Find candidates near start point
            start_buffer = start_point.buffer(search_radius)
            start_candidates_idx = list(spatial_index.intersection(start_buffer.bounds))
            start_candidates = all_features_proj.iloc[start_candidates_idx]
            start_candidates = start_candidates[start_candidates["id"] != feature_id]

            # Find candidates near end point
            end_buffer = end_point.buffer(search_radius)
            end_candidates_idx = list(spatial_index.intersection(end_buffer.bounds))
            end_candidates = all_features_proj.iloc[end_candidates_idx]
            end_candidates = end_candidates[end_candidates["id"] != feature_id]

            # Get connections for both endpoints
            start_conns = get_endpoint_connections(
                start_point,
                start_candidates,
                layer_config,
                layer_key,
                max_connections=2,
            )
            end_conns = get_endpoint_connections(
                end_point, end_candidates, layer_config, layer_key, max_connections=2
            )

            # Combine and deduplicate
            all_conns = list(set(start_conns + end_conns))
            connections_list.append(all_conns)

        else:
            # For points, check centroid
            point = geom if geom.geom_type == "Point" else geom.centroid

            buffer = point.buffer(search_radius)
            candidates_idx = list(spatial_index.intersection(buffer.bounds))
            candidates = all_features_proj.iloc[candidates_idx]
            candidates = candidates[candidates["id"] != feature_id]

            # Determine max connections based on layer type
            max_conns = (
                100
                if layer_key.startswith("postes")
                else (5 if layer_key.endswith("bt") else 20)
            )

            connections = get_endpoint_connections(
                point, candidates, layer_config, layer_key, max_connections=max_conns
            )
            connections_list.append(connections)

    # Add connections to the GeoDataFrame
    gdf_proj["connections"] = connections_list
    result = gdf_proj.to_crs(gdf.crs)

    total_connections = sum(len(c) for c in connections_list)
    logger.info(
        f"Layer {layer_key}: {len(gdf)} features, {total_connections} connections"
    )

    return result


def process_all_connections(
    layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, gpd.GeoDataFrame]:
    """Process connections for all layers following electrical grid hierarchy."""
    logger.info("Processing electrical grid connections...")

    # Combine all features for spatial queries
    all_features = gpd.GeoDataFrame(
        pd.concat(layers.values(), ignore_index=True), crs=list(layers.values())[0].crs
    )

    # Process layers in priority order
    priority_order = sorted(layers.keys(), key=lambda x: LAYERS[x].priority)
    updated_layers = {}

    for layer_key in priority_order:
        logger.info(f"Processing connections for {layer_key}")

        if layer_key != "reseau_souterrain_bt":
            logger.info(f"Skipping {layer_key} as it is not processed in this step.")
            continue

        gdf = layers[layer_key]
        updated_gdf = calculate_layer_connections(gdf, all_features, layer_key)
        updated_layers[layer_key] = updated_gdf
        # Save each processed layer immediately
        try:
            save_to_local_storage(updated_gdf, layer_key)
        except Exception as e:
            logger.error(f"Failed to save processed layer {layer_key} immediately: {e}")

    return updated_layers
