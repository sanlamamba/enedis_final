"""Optimized connections calculation for electrical grid components."""

import logging
from typing import Dict, List
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import time
from config import LAYERS
from exporter import save_layer

logger = logging.getLogger(__name__)


def load_processed_layer(layer_key: str) -> gpd.GeoDataFrame:
    """Load a previously processed layer from the individual output folder."""
    from pathlib import Path

    individual_path = Path("../output/individual")
    layer_file = individual_path / f"{layer_key}.geojson"

    if layer_file.exists():
        try:
            gdf = gpd.read_file(layer_file)
            logger.info(
                f"Loaded previously processed layer {layer_key} from {layer_file}"
            )
            return gdf
        except Exception as e:
            logger.warning(f"Failed to load {layer_key} from individual folder: {e}")
            return None
    return None


def check_processed_layers() -> Dict[str, gpd.GeoDataFrame]:
    """Check which layers have already been processed and load them."""
    processed_layers = {}

    for layer_key in LAYERS.keys():
        processed_gdf = load_processed_layer(layer_key)
        if processed_gdf is not None:
            processed_layers[layer_key] = processed_gdf

    if processed_layers:
        logger.info(
            f"Found {len(processed_layers)} previously processed layers: {list(processed_layers.keys())}"
        )

    return processed_layers


def cleanup_individual_files():
    """Clean up individual layer files after successful completion."""
    from pathlib import Path
    import os

    individual_path = Path("../output/individual")

    if individual_path.exists():
        try:
            for file in individual_path.glob("*.geojson"):
                os.remove(file)
                logger.info(f"Cleaned up individual file: {file}")

            # Remove the directory if it's empty
            if not any(individual_path.iterdir()):
                individual_path.rmdir()
                logger.info("Removed empty individual directory")

        except Exception as e:
            logger.warning(f"Failed to clean up individual files: {e}")


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

    distances = candidates.geometry.distance(endpoint)
    candidates = candidates.copy()
    candidates["distance"] = distances
    candidates = candidates[candidates["distance"] <= layer_config.connection_radius]

    if candidates.empty:
        return []

    allowed_layers = set(layer_config.can_connect_to)
    candidates = candidates[candidates["layer"].isin(allowed_layers)]

    if candidates.empty:
        return []

    candidates["priority"] = candidates["layer"].map(
        lambda x: LAYERS[x].priority if x in LAYERS else 999
    )
    candidates = candidates.sort_values(["priority", "distance"])

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

    gdf_proj = gdf.to_crs("EPSG:3857")
    all_features_proj = all_features.to_crs("EPSG:3857")

    spatial_index = all_features_proj.sindex

    connections_list = []

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

        else:
            point = geom if geom.geom_type == "Point" else geom.centroid

            buffer = point.buffer(search_radius)
            candidates_idx = list(spatial_index.intersection(buffer.bounds))
            candidates = all_features_proj.iloc[candidates_idx]
            candidates = candidates[candidates["id"] != feature_id]

            max_conns = (
                100
                if layer_key.startswith("postes")
                else (5 if layer_key.endswith("bt") else 20)
            )

            connections = get_endpoint_connections(
                point, candidates, layer_config, layer_key, max_connections=max_conns
            )
            connections_list.append(connections)

    gdf_proj["connections"] = connections_list
    result = gdf_proj.to_crs(gdf.crs)

    total_connections = sum(len(c) for c in connections_list)
    logger.info(
        f"Layer {layer_key}: {len(gdf)} features, {total_connections} connections"
    )

    return result


def process_all_connections(
    layers: Dict[str, gpd.GeoDataFrame], resume_from_individual: bool = True
) -> Dict[str, gpd.GeoDataFrame]:
    """Process connections for all layers following electrical grid hierarchy.

    Args:
        layers: Dictionary of layers to process
        resume_from_individual: Whether to resume from previously processed individual layers
    """
    logger.info("Processing electrical grid connections...")

    # Check for previously processed layers if resuming is enabled
    processed_layers = {}
    if resume_from_individual:
        processed_layers = check_processed_layers()
        if processed_layers:
            logger.info(
                f"Resuming from {len(processed_layers)} previously processed layers"
            )

    all_features = gpd.GeoDataFrame(
        pd.concat(layers.values(), ignore_index=True), crs=list(layers.values())[0].crs
    )

    priority_order = sorted(layers.keys(), key=lambda x: LAYERS[x].priority)
    updated_layers = {}

    for layer_key in priority_order:
        # Check if this layer was already processed
        if layer_key in processed_layers:
            logger.info(f"Using previously processed layer: {layer_key}")
            updated_layers[layer_key] = processed_layers[layer_key]
            continue

        logger.info(f"Processing connections for {layer_key}")
        gdf = layers[layer_key]
        # print layer key
        print(f"Processing connections for {layer_key}")
        time.sleep(20)  # Simulate processing time

        try:
            updated_gdf = calculate_layer_connections(gdf, all_features, layer_key)
            updated_layers[layer_key] = updated_gdf

            # Save the updated layer to an output_individual folder immediately
            output_path = save_layer(
                updated_gdf, layer_key, destination="local", output_dir="individual"
            )
            logger.info(f"Saved individual layer {layer_key} to {output_path}")

        except Exception as e:
            logger.error(f"Failed to process layer {layer_key}: {e}")
            # If processing fails, try to use the original layer without connections
            logger.warning(f"Using original layer {layer_key} without connections")
            gdf_copy = gdf.copy()
            gdf_copy["connections"] = [[] for _ in range(len(gdf_copy))]
            updated_layers[layer_key] = gdf_copy

    return updated_layers
