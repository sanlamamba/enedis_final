"""
Spatial connection calculation module.

This module parallelizes the calculation of connections between entities
using a process pool and spatial algorithms.
"""

import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Set, Tuple, Optional, Any, Union
from functools import partial
import os

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString

from config import LAYERS_CONFIG
from utils import timed


# Shared variables for worker processes
_global_all_features_proj = None
_global_spatial_index = None
_global_base_radius = None
_global_exclude_list = None
_global_priority_connections = None
_global_mono_connection = None


def init_worker(
    all_features_proj, base_radius, exclude_list, priority_connections, mono_connection
):
    """
    Initialize global variables in each worker process.

    Args:
        all_features_proj: All projected entities (CRS in EPSG:3857) for spatial search.
        base_radius: Base radius used for buffer around a point during search.
        exclude_list: List of layer names to exclude when searching for connections.
        priority_connections: Dictionary of priorities for certain layers.
            Example: {'layer_name': {'priority': 1, 'radius': 7}}
        mono_connection: Indicates if only one connection per endpoint should be selected.
    """
    global _global_all_features_proj, _global_spatial_index, _global_base_radius
    global _global_exclude_list, _global_priority_connections, _global_mono_connection

    _global_all_features_proj = all_features_proj
    _global_spatial_index = all_features_proj.sindex
    _global_base_radius = base_radius
    _global_exclude_list = exclude_list
    _global_priority_connections = priority_connections
    _global_mono_connection = mono_connection


def select_connection_candidates(
    endpoint: Point,
    all_features_proj: gpd.GeoDataFrame,
    spatial_index,
    base_radius: float,
    exclude_list: Optional[List[str]],
    priority_connections: Optional[Dict[str, Dict[str, Any]]],
    mono_connection: bool,
    solo_dict: Optional[Dict[str, Dict[str, Any]]],
) -> Set[str]:
    """
    Select candidate connection IDs for a given point.

    Args:
        endpoint: Point or geometric object serving as reference.
        all_features_proj: Complete set of entities, projected in EPSG:3857.
        spatial_index: Spatial index built on all_features_proj to accelerate spatial queries.
        base_radius: Radius for creating buffer around endpoint.
        exclude_list: List of layer names to exclude from search.
        priority_connections: Dictionary of priorities and radii for certain layers.
        mono_connection: If True, selects a single connection per endpoint based on priority.
        solo_dict: Optional. Dictionary indicating special rules for restricting connections.

    Returns:
        Set of identifiers (strings) for candidates selected as connections.
    """
    # Create a buffer around the endpoint
    buf = endpoint.buffer(base_radius)

    # Use spatial index for efficient querying
    possible_idx = list(spatial_index.intersection(buf.bounds))

    # Skip if no candidates
    if not possible_idx:
        return set()

    possible = all_features_proj.iloc[possible_idx]

    # Further filter by actual intersection
    matches = possible[possible.intersects(buf)]

    # Apply exclusion filter
    if exclude_list and not matches.empty:
        matches = matches[~matches["source_layer"].isin(exclude_list)]

    # Handle solo connection case
    if solo_dict is not None and not matches.empty:
        candidates = matches[matches["source_layer"].isin(solo_dict.keys())].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)
            valid = candidates[
                candidates.apply(
                    lambda row: row["distance"]
                    <= solo_dict[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]
            if not valid.empty:
                valid = valid.sort_values(by="distance")
                return {valid.iloc[0]["id"]}

    # Handle mono connection case
    if mono_connection and priority_connections and not matches.empty:
        candidates = matches[
            matches["source_layer"].isin(priority_connections.keys())
        ].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)

            # Filter by maximum connection radius
            valid = candidates[
                candidates.apply(
                    lambda row: row["distance"]
                    <= priority_connections[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]

            if not valid.empty:
                valid["priority"] = valid["source_layer"].map(
                    lambda s: priority_connections[s]["priority"]
                )
                # Sort by priority then distance
                valid = valid.sort_values(by=["priority", "distance"])
                return {valid.iloc[0]["id"]}

    # Default case: return all matching IDs
    return set(matches["id"].tolist())


def process_feature_worker(
    item: Tuple[int, pd.Series],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Process a feature to calculate its spatial connections.

    Args:
        item: Tuple (idx, feature) where 'idx' is the index and 'feature' a row of the GeoDataFrame.

    Returns:
        Three lists corresponding to:
            - Union of connections (start + end)
            - Connections at start point
            - Connections at end point
    """
    idx, feature = item
    all_features_proj = _global_all_features_proj
    spatial_index = _global_spatial_index
    base_radius = _global_base_radius
    exclude_list = _global_exclude_list
    priority_connections = _global_priority_connections
    mono_connection = _global_mono_connection

    geom = feature.geometry
    feature_id = feature["id"]

    # Special handling for LineString
    if geom.geom_type == "LineString":
        # Extract start and end points
        start_point = Point(geom.coords[0])
        end_point = Point(geom.coords[-1])

        # Get layer config
        config = LAYERS_CONFIG.get(feature["source_layer"], {})
        solo_dict = getattr(config, "solo_connection_if", None)

        # Find connections at start point
        start_ids = select_connection_candidates(
            start_point,
            all_features_proj,
            spatial_index,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict,
        )

        # Find connections at end point
        end_ids = select_connection_candidates(
            end_point,
            all_features_proj,
            spatial_index,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict,
        )

        # Remove self-connections
        start_ids.discard(feature_id)
        end_ids.discard(feature_id)

        # Union of all connections
        union_ids = start_ids.union(end_ids)

        return list(union_ids), list(start_ids), list(end_ids)

    # Handling for Point or other geometry types
    else:
        endpoint = geom if geom.geom_type == "Point" else geom.centroid
        candidate_ids = select_connection_candidates(
            endpoint,
            all_features_proj,
            spatial_index,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict=None,
        )

        # Remove self-connections
        candidate_ids.discard(feature_id)

        return list(candidate_ids), [], []


@timed
def find_connections(
    gdf: gpd.GeoDataFrame,
    all_features: gpd.GeoDataFrame,
    base_radius: float,
    exclude_list: Optional[List[str]] = None,
    priority_connections: Optional[Dict[str, Dict[str, Any]]] = None,
    mono_connection_per_endpoint: bool = False,
) -> gpd.GeoDataFrame:
    """
    Calculate spatial connections for each entity in a GeoDataFrame.

    Args:
        gdf: Set of entities to process.
        all_features: Complete set of entities (for connection search).
        base_radius: Base radius for connection search.
        exclude_list: List of layers to exclude.
        priority_connections: Dictionary of priorities and radii for certain layers.
        mono_connection_per_endpoint: If True, limits connection to one per endpoint.

    Returns:
        The original GeoDataFrame with three added columns:
            - "connections": list of connection IDs (union of start and end connections)
            - "start_connections": list of connections at start point (for LineStrings)
            - "end_connections": list of connections at end point (for LineStrings)
    """
    if exclude_list is None:
        exclude_list = []

    # Project dataframes to a metric CRS for accurate spatial analysis
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    # Process features in parallel
    with ProcessPoolExecutor(
        max_workers=min(os.cpu_count(), 8),  # Limit workers to prevent memory issues
        initializer=init_worker,
        initargs=(
            all_features_proj,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection_per_endpoint,
        ),
    ) as executor:
        # Use list to materialize results
        results = list(
            executor.map(
                process_feature_worker,
                list(gdf_proj.iterrows()),
                chunksize=max(
                    1, len(gdf_proj) // (os.cpu_count() * 2)
                ),  # Optimize chunk size
            )
        )

    # Unpack results
    connections, start_connections, end_connections = zip(*results)

    # Add connection data to the dataframe
    gdf_proj["connections"] = connections
    gdf_proj["start_connections"] = start_connections
    gdf_proj["end_connections"] = end_connections

    # Return projected back to WGS84
    return gdf_proj.to_crs(epsg=4326)


@timed
def compute_connections(
    layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Reproject GeoDataFrames to EPSG:4326, concatenate them to create a global GeoDataFrame,
    and calculate spatial connections for each layer.

    Args:
        layers: Dictionary of GeoDataFrames for each layer.

    Returns:
        Updated dictionary associating each layer with a GeoDataFrame enriched with
        "connections", "start_connections" and "end_connections" columns.
    """
    if not layers:
        logging.warning("No layers to process for connections")
        return {}

    # Reproject all layers to EPSG:4326
    reprojected_layers = {}
    for layer_key, gdf in layers.items():
        if "source_layer" not in gdf.columns:
            gdf["source_layer"] = layer_key

        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")

        reprojected_layers[layer_key] = gdf

    # Concatenate all layers into one GeoDataFrame
    all_features = gpd.GeoDataFrame(
        pd.concat(list(reprojected_layers.values()), ignore_index=True), crs="EPSG:4326"
    )

    # Optimize with rtree spatial index
    all_features.sindex

    # Process each layer
    updated_layers = {}
    for layer_key, gdf in layers.items():
        cfg = LAYERS_CONFIG.get(layer_key, {})

        # Get connection configuration
        exclude = getattr(cfg, "exclude_connections", [])
        priority = getattr(cfg, "priority_connections", None)
        mono = getattr(cfg, "mono_connection_per_endpoint", False)
        radius = getattr(cfg, "radius", 3)

        # Calculate connections
        updated_gdf = find_connections(
            gdf, all_features, radius, exclude, priority, mono
        )
        updated_layers[layer_key] = updated_gdf

    return updated_layers


def optimize_connections(
    layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Optimize the connection links to reduce redundancy and improve network topology.

    Args:
        layers: Dictionary of GeoDataFrames with calculated connections

    Returns:
        Dictionary with optimized connection links
    """
    optimized_layers = {}

    # Build a network graph to analyze connection patterns
    import networkx as nx

    G = nx.Graph()

    # Add all nodes and edges to the graph
    for layer_key, gdf in layers.items():
        for _, feature in gdf.iterrows():
            node_id = feature["id"]
            G.add_node(node_id, layer=layer_key)

            for connection in feature.get("connections", []):
                G.add_edge(node_id, connection)

    # Analyze and optimize the network
    for layer_key, gdf in layers.items():
        # Copy to avoid modifying original
        opt_gdf = gdf.copy()

        for idx, feature in opt_gdf.iterrows():
            node_id = feature["id"]

            # Keep only important connections (e.g., remove transitive connections)
            if node_id in G:
                # For certain layer types, we may want to apply specific optimizations
                connections = feature.get("connections", [])

                # Example optimization: Remove redundant connections
                # This is a simple example - actual optimization would depend on domain requirements
                if len(connections) > 10:  # If too many connections
                    # Keep only the closest connections
                    optimized_connections = connections[:10]
                    opt_gdf.at[idx, "connections"] = optimized_connections

        optimized_layers[layer_key] = opt_gdf

    return optimized_layers
