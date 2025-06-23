"""Calculate connections between electrical grid components."""

import logging
from typing import Dict, List, Set, Tuple
import geopandas as gpd
from shapely.geometry import Point, LineString
import pandas as pd

from config import LAYERS

logger = logging.getLogger(__name__)


def get_endpoint_connections(
    endpoint: Point,
    candidates: gpd.GeoDataFrame,
    layer_config,
    feature_layer: str,
    max_connections: int = 1,
) -> List[str]:
    """Find connections for a single endpoint based on electrical grid hierarchy."""
    if candidates.empty:
        return []

    distances = candidates.geometry.distance(endpoint)
    candidates = candidates.copy()
    candidates["distance"] = distances

    candidates = candidates[candidates["distance"] <= layer_config.connection_radius]

    if candidates.empty:
        return []

    valid_connections = []

    allowed_layers = set(layer_config.can_connect_to)

    candidates = candidates[candidates["layer"].isin(allowed_layers)]

    if candidates.empty:
        return []

    candidates["priority"] = candidates["layer"].map(
        lambda x: LAYERS[x].priority if x in LAYERS else 999
    )
    candidates = candidates.sort_values(["priority", "distance"])

    for _, candidate in candidates.iterrows():
        candidate_layer = candidate["layer"]
        candidate_config = LAYERS.get(candidate_layer)

        if not candidate_config:
            continue

        if feature_layer in candidate_config.can_connect_to:
            valid_connections.append(candidate["id"])

            if len(valid_connections) >= max_connections:
                break

    return valid_connections


def calculate_connections(
    gdf: gpd.GeoDataFrame, all_features: gpd.GeoDataFrame, layer_key: str
) -> gpd.GeoDataFrame:
    """Calculate connections for all features in a layer."""
    layer_config = LAYERS[layer_key]

    gdf_proj = gdf.to_crs("EPSG:3857")
    all_features_proj = all_features.to_crs("EPSG:3857")

    spatial_index = all_features_proj.sindex

    connections_list = []
    start_connections_list = []
    end_connections_list = []

    for idx, feature in gdf_proj.iterrows():
        geom = feature.geometry
        feature_id = feature["id"]

        search_radius = layer_config.connection_radius * 1.5

        if geom.geom_type == "LineString":
            start_point = Point(geom.coords[0])
            end_point = Point(geom.coords[-1])

            start_buffer = start_point.buffer(search_radius)
            start_candidates_idx = list(spatial_index.intersection(start_buffer.bounds))
            start_candidates = all_features_proj.iloc[start_candidates_idx]
            start_candidates = start_candidates[start_candidates["id"] != feature_id]

            end_buffer = end_point.buffer(search_radius)
            end_candidates_idx = list(spatial_index.intersection(end_buffer.bounds))
            end_candidates = all_features_proj.iloc[end_candidates_idx]
            end_candidates = end_candidates[end_candidates["id"] != feature_id]

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

            all_conns = list(set(start_conns + end_conns))

            connections_list.append(all_conns)
            start_connections_list.append(start_conns)
            end_connections_list.append(end_conns)

        else:
            point = geom if geom.geom_type == "Point" else geom.centroid

            buffer = point.buffer(search_radius)
            candidates_idx = list(spatial_index.intersection(buffer.bounds))
            candidates = all_features_proj.iloc[candidates_idx]
            candidates = candidates[candidates["id"] != feature_id]

            connections = get_endpoint_connections(
                point,
                candidates,
                layer_config,
                layer_key,
                max_connections=(
                    100
                    if layer_key.startswith("postes")
                    else (2 if layer_key.endswith("bt") else 50)
                ),
            )

            connections_list.append(connections)
            start_connections_list.append([])
            end_connections_list.append([])

    gdf_proj["connections"] = connections_list
    gdf_proj["start_connections"] = start_connections_list
    gdf_proj["end_connections"] = end_connections_list

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
    all_features = gpd.GeoDataFrame(
        pd.concat(layers.values(), ignore_index=True), crs=list(layers.values())[0].crs
    )

    priority_order = sorted(layers.keys(), key=lambda x: LAYERS[x].priority)

    updated_layers = {}

    for layer_key in priority_order:
        logger.info(f"Processing connections for {layer_key}")
        gdf = layers[layer_key]
        updated_gdf = calculate_connections(gdf, all_features, layer_key)
        updated_layers[layer_key] = updated_gdf

    validate_connections(updated_layers)

    return updated_layers


def validate_connections(layers: Dict[str, gpd.GeoDataFrame]):
    """Validate that connections follow electrical grid rules."""
    all_ids = set()
    for gdf in layers.values():
        all_ids.update(gdf["id"].tolist())

    issues = []

    for layer_key, gdf in layers.items():
        layer_config = LAYERS[layer_key]

        for _, feature in gdf.iterrows():
            for conn_id in feature["connections"]:
                if conn_id not in all_ids:
                    issues.append(f"Invalid connection {conn_id} in {feature['id']}")

            for conn_id in feature["connections"]:
                conn_layer = None
                for other_key, other_gdf in layers.items():
                    if conn_id in other_gdf["id"].values:
                        conn_layer = other_key
                        break

                if conn_layer and conn_layer not in layer_config.can_connect_to:
                    issues.append(
                        f"Invalid connection type: {layer_key} -> {conn_layer} "
                        f"(feature {feature['id']} -> {conn_id})"
                    )

    if issues:
        logger.warning(f"Found {len(issues)} connection issues")
        for issue in issues[:10]:
            logger.warning(issue)
