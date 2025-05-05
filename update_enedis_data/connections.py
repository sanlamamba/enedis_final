"""
Spatial connection calculation module.

This module parallelizes the calculation of connections between entities
using a process pool and spatial algorithms.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple, Optional, Any, Union, NamedTuple
from functools import partial
import os
import time
import multiprocessing
from dataclasses import dataclass
import heapq

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points

from config import LAYERS_CONFIG, CONNECTION_RADIUS, RadiusType
from utils import timed

# Shared variables for worker processes
_global_all_features_proj = None
_global_spatial_index = None
_global_base_radius = None
_global_exclude_list = None
_global_priority_connections = None
_global_mono_connection = None


@dataclass
class ConnectionParams:
    """Parameters for connection calculation that are passed to worker processes."""

    all_features_proj: gpd.GeoDataFrame
    base_radius: float
    exclude_list: Optional[List[str]]
    priority_connections: Optional[Dict[str, Dict[str, Any]]]
    mono_connection: bool


@dataclass
class ConnectionCandidate:
    """Represents a potential connection with priority and distance information."""

    id: str
    source_layer: str
    distance: float
    priority: int = 999  # Default low priority

    def __lt__(self, other):
        # For priority queue ordering: first by priority, then by distance
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.distance < other.distance


class ConnectionParamsManager:
    """Thread-local storage for connection parameters."""

    _instance = None
    _params = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConnectionParamsManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def set_params(cls, params: ConnectionParams):
        cls._params = params

    @classmethod
    def get_params(cls) -> ConnectionParams:
        return cls._params


def init_worker(params: ConnectionParams):
    """
    Initialize worker process with connection parameters.

    Args:
        params: Connection parameters for calculations
    """
    ConnectionParamsManager.set_params(params)


def select_connection_candidates(
    endpoint: Point,
    spatial_index,
    solo_dict: Optional[Dict[str, Dict[str, Any]]] = None,
    max_connections: int = 50,
) -> List[ConnectionCandidate]:
    """
    Select candidate connection IDs for a given point with improved selection logic.

    Args:
        endpoint: Point or geometric object serving as reference.
        spatial_index: Spatial index for rapid feature lookup
        solo_dict: Optional. Dictionary indicating special rules for restricting connections.
        max_connections: Maximum number of connections to return per endpoint

    Returns:
        List of ConnectionCandidate objects representing potential connections
    """
    # Get connection parameters from thread-local storage
    params = ConnectionParamsManager.get_params()
    all_features_proj = params.all_features_proj
    base_radius = params.base_radius
    exclude_list = params.exclude_list
    priority_connections = params.priority_connections
    mono_connection = params.mono_connection

    # First try with base radius
    candidates = _search_with_radius(
        endpoint,
        base_radius,
        spatial_index,
        all_features_proj,
        exclude_list,
        priority_connections,
        mono_connection,
        solo_dict,
        max_connections,
    )

    # If no candidates found and we have priority connections, try with FAR radius
    if not candidates and priority_connections and mono_connection:
        # For BT components, try with FAR radius
        far_radius = CONNECTION_RADIUS[RadiusType.FAR]
        candidates = _search_with_radius(
            endpoint,
            far_radius,
            spatial_index,
            all_features_proj,
            exclude_list,
            priority_connections,
            mono_connection,
            solo_dict,
            max_connections,
        )

    return candidates


def _search_with_radius(
    endpoint: Point,
    search_radius: float,
    spatial_index,
    all_features_proj: gpd.GeoDataFrame,
    exclude_list: Optional[List[str]],
    priority_connections: Optional[Dict[str, Dict[str, Any]]],
    mono_connection: bool,
    solo_dict: Optional[Dict[str, Dict[str, Any]]],
    max_connections: int,
) -> List[ConnectionCandidate]:
    """
    Search for connection candidates with a specific radius.
    """
    # Create a buffer around the endpoint
    buf = endpoint.buffer(search_radius)

    # Use spatial index for efficient querying
    possible_idx = list(spatial_index.intersection(buf.bounds))

    # Skip if no candidates
    if not possible_idx:
        return []

    possible = all_features_proj.iloc[possible_idx]

    # Further filter by actual intersection
    matches = possible[possible.intersects(buf)]

    # Apply exclusion filter
    if exclude_list and not matches.empty:
        matches = matches[~matches["source_layer"].isin(exclude_list)]

    if matches.empty:
        return []

    # Handle special case for solo connections
    if solo_dict is not None and not matches.empty:
        candidates = matches[matches["source_layer"].isin(solo_dict.keys())].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)
            # Filter by radius for each source layer
            valid_candidates = []

            for _, row in candidates.iterrows():
                source_layer = row["source_layer"]
                if source_layer in solo_dict:
                    max_radius = solo_dict[source_layer]["radius"]
                    if row["distance"] <= max_radius:
                        priority = solo_dict[source_layer].get("priority", 999)
                        valid_candidates.append(
                            ConnectionCandidate(
                                id=row["id"],
                                source_layer=source_layer,
                                distance=row["distance"],
                                priority=priority,
                            )
                        )

            if valid_candidates:
                # Sort by priority then distance
                valid_candidates.sort()
                return [valid_candidates[0]]  # Return only the best candidate

    # Handle mono_connection case with priority
    if mono_connection and priority_connections and not matches.empty:
        candidates = matches[
            matches["source_layer"].isin(priority_connections.keys())
        ].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)

            # Create a list of valid connection candidates
            valid_candidates = []

            for _, row in candidates.iterrows():
                source_layer = row["source_layer"]
                layer_config = priority_connections.get(source_layer, {})
                # Use specific radius if defined, otherwise use default search radius
                max_radius = layer_config.get("radius", search_radius)

                if row["distance"] <= max_radius:
                    priority = layer_config.get("priority", 999)
                    valid_candidates.append(
                        ConnectionCandidate(
                            id=row["id"],
                            source_layer=source_layer,
                            distance=row["distance"],
                            priority=priority,
                        )
                    )

            if valid_candidates:
                # Sort by priority then distance
                valid_candidates.sort()
                return [valid_candidates[0]]  # Return only the highest priority

    # Default case: select multiple connections based on distance
    candidates = []

    # Calculate distances for all matches
    match_distances = []
    for _, row in matches.iterrows():
        distance = row.geometry.distance(endpoint)
        match_distances.append((row, distance))

    # Sort by distance
    match_distances.sort(key=lambda x: x[1])

    # Take up to max_connections closest matches
    for row, distance in match_distances[:max_connections]:
        candidates.append(
            ConnectionCandidate(
                id=row["id"], source_layer=row["source_layer"], distance=distance
            )
        )

    return candidates


def process_feature_worker(
    item: Tuple[int, pd.Series], max_connections_per_endpoint: int = 10
) -> Tuple[List[str], List[str], List[str]]:
    """
    Process a feature to calculate its spatial connections.

    Args:
        item: Tuple (idx, feature) where 'idx' is the index and 'feature' a row of the GeoDataFrame.
        max_connections_per_endpoint: Maximum number of connections to keep per endpoint

    Returns:
        Three lists corresponding to:
            - Union of connections (start + end)
            - Connections at start point
            - Connections at end point
    """
    idx, feature = item
    params = ConnectionParamsManager.get_params()
    all_features_proj = params.all_features_proj
    spatial_index = all_features_proj.sindex

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

        start_candidates = select_connection_candidates(
            start_point, spatial_index, solo_dict, max_connections_per_endpoint
        )
        start_ids = [c.id for c in start_candidates]

        end_candidates = select_connection_candidates(
            end_point, spatial_index, solo_dict, max_connections_per_endpoint
        )
        end_ids = [c.id for c in end_candidates]

        if feature_id in start_ids:
            start_ids.remove(feature_id)
        if feature_id in end_ids:
            end_ids.remove(feature_id)

        union_ids = list(set(start_ids + end_ids))

        return union_ids, start_ids, end_ids

    else:
        endpoint = geom if geom.geom_type == "Point" else geom.centroid
        candidates = select_connection_candidates(
            endpoint,
            spatial_index,
            solo_dict=None,
            max_connections=max_connections_per_endpoint,
        )
        candidate_ids = [c.id for c in candidates]

        if feature_id in candidate_ids:
            candidate_ids.remove(feature_id)

        return candidate_ids, [], []


@timed
def find_connections(
    gdf: gpd.GeoDataFrame,
    all_features: gpd.GeoDataFrame,
    base_radius: float,
    exclude_list: Optional[List[str]] = None,
    priority_connections: Optional[Dict[str, Dict[str, Any]]] = None,
    mono_connection_per_endpoint: bool = False,
    max_connections_per_endpoint: int = 10,
    chunk_size: int = 500,
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
        max_connections_per_endpoint: Maximum connections to keep per endpoint.
        chunk_size: Size of chunks for parallel processing.

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

    # Ensure we have spatial indexes for efficient spatial queries
    if not hasattr(all_features_proj, "sindex") or all_features_proj.sindex is None:
        all_features_proj.sindex

    # Create connection parameters to pass to worker processes
    conn_params = ConnectionParams(
        all_features_proj=all_features_proj,
        base_radius=base_radius,
        exclude_list=exclude_list,
        priority_connections=priority_connections,
        mono_connection=mono_connection_per_endpoint,
    )

    # Calculate optimal number of workers and chunk size
    num_cores = min(os.cpu_count() or 1, 16)  # Limit to 16 cores max
    num_features = len(gdf_proj)

    # Adjust chunk size based on dataset size
    if num_features <= 1000:
        # Small dataset - process in a single chunk or use very few workers
        chunk_size = (
            max(1, num_features // (num_cores * 2))
            if num_features > 100
            else num_features
        )
        num_workers = min(num_cores, max(1, num_features // chunk_size))
    else:
        # Larger dataset - use more workers but with reasonable chunk sizes
        num_workers = num_cores
        chunk_size = max(chunk_size, num_features // (num_cores * 4))

    # Process features in parallel
    all_results = []

    # Use chunking to avoid memory issues with very large datasets
    for i in range(0, num_features, chunk_size):
        end_idx = min(i + chunk_size, num_features)
        chunk = gdf_proj.iloc[i:end_idx]

        # Process this chunk
        with ProcessPoolExecutor(
            max_workers=num_workers, initializer=init_worker, initargs=(conn_params,)
        ) as executor:
            # Create a list of futures for this chunk
            futures = []
            for idx, row in chunk.iterrows():
                futures.append(
                    executor.submit(
                        process_feature_worker, (idx, row), max_connections_per_endpoint
                    )
                )

            # Process results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    all_results.append(result)
                except Exception as e:
                    logging.error(f"Error processing feature: {e}")
                    # Add empty result to maintain order
                    all_results.append(([], [], []))

    # Unpack results
    if len(all_results) < len(gdf_proj):
        # Fill missing results with empty lists if needed
        missing = len(gdf_proj) - len(all_results)
        all_results.extend([([], [], [])] * missing)
        logging.warning(f"Added {missing} empty results to match dataframe length")

    connections, start_connections, end_connections = zip(*all_results)

    # Add connection data to the dataframe
    gdf_proj["connections"] = connections
    gdf_proj["start_connections"] = start_connections
    gdf_proj["end_connections"] = end_connections

    # Do additional validation on connections
    gdf_proj = validate_connections(gdf_proj, all_features_proj)

    # Return projected back to WGS84
    return gdf_proj.to_crs(epsg=4326)


def validate_connections(
    gdf: gpd.GeoDataFrame, all_features: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Validate and clean up connections to ensure network integrity.

    Args:
        gdf: GeoDataFrame with calculated connections
        all_features: Complete set of features for validation

    Returns:
        Validated and cleaned GeoDataFrame
    """
    # Create set of all valid feature IDs
    valid_ids = set(all_features["id"].unique())

    # Validate each row's connections
    for idx, row in gdf.iterrows():
        # Filter connections to only include existing features
        connections = [c for c in row.get("connections", []) if c in valid_ids]
        start_connections = [
            c for c in row.get("start_connections", []) if c in valid_ids
        ]
        end_connections = [c for c in row.get("end_connections", []) if c in valid_ids]

        # Remove self-references
        feature_id = row["id"]
        if feature_id in connections:
            connections.remove(feature_id)
        if feature_id in start_connections:
            start_connections.remove(feature_id)
        if feature_id in end_connections:
            end_connections.remove(feature_id)

        # Limit number of connections if excessive (more than 60 is likely noise)
        if len(connections) > 60:
            logging.warning(
                f"Feature {feature_id} has {len(connections)} connections - limiting to 60"
            )
            # For excessive connections, prioritize by proximity
            # This would require distance calculation which is computationally expensive
            # So we'll just take the first 15 for now which are likely from start/end points
            connections = connections[:60]

        # Update the dataframe
        gdf.at[idx, "connections"] = connections
        gdf.at[idx, "start_connections"] = start_connections
        gdf.at[idx, "end_connections"] = end_connections

    return gdf


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

    # First create a lightweight version of all features for spatial indexing
    # to reduce memory usage during connection calculation
    minimal_features = []
    for layer_key, gdf in reprojected_layers.items():
        minimal_gdf = gdf[["id", "geometry", "source_layer"]].copy()
        minimal_features.append(minimal_gdf)

    all_features = gpd.GeoDataFrame(
        pd.concat(minimal_features, ignore_index=True), crs="EPSG:4326"
    )

    # Optimize with rtree spatial index
    if not hasattr(all_features, "sindex") or all_features.sindex is None:
        all_features.sindex

    # Process each layer
    updated_layers = {}
    total_features = sum(len(gdf) for gdf in layers.values())

    # Process larger layers first to optimize parallel processing
    sorted_layers = sorted(
        [(k, len(gdf)) for k, gdf in layers.items()], key=lambda x: x[1], reverse=True
    )

    for layer_key, feature_count in sorted_layers:
        gdf = reprojected_layers[layer_key]
        if gdf.empty:
            logging.warning(
                f"Layer {layer_key} is empty - skipping connection calculation"
            )
            updated_layers[layer_key] = gdf
            continue

        cfg = LAYERS_CONFIG.get(layer_key, {})

        exclude = getattr(cfg, "exclude_connections", [])
        priority = getattr(cfg, "priority_connections", None)
        mono = getattr(cfg, "mono_connection_per_endpoint", False)
        radius = getattr(cfg, "radius", 3)

        if feature_count < 1000:
            chunk_size = feature_count
        elif feature_count < 5000:
            chunk_size = 1000
        elif feature_count < 20000:
            chunk_size = 2000
        else:
            chunk_size = 5000

        max_connections = 60 if layer_key.startswith("reseau") else 30
        max_connections = 1 if layer_key.endswith("bt") else max_connections

        try:
            logging.info(
                f"Calculating connections for layer {layer_key} with {feature_count} features"
            )
            updated_gdf = find_connections(
                gdf,
                all_features,
                radius,
                exclude,
                priority,
                mono,
                max_connections_per_endpoint=max_connections,
                chunk_size=chunk_size,
            )
            updated_layers[layer_key] = updated_gdf
            logging.info(f"Completed connection calculation for {layer_key}")
        except Exception as e:
            logging.error(f"Error processing connections for layer {layer_key}: {e}")
            updated_layers[layer_key] = gdf

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

            # Skip if node isn't in graph
            if node_id not in G:
                continue

            connections = feature.get("connections", [])

            # No need to optimize if few connections
            if len(connections) <= 5:
                continue

            # Check if this is a line-type feature
            if feature.geometry.geom_type == "LineString":
                # For lines, we want to maintain connectivity at endpoints
                # but reduce redundancy in the middle
                start_conn = feature.get("start_connections", [])
                end_conn = feature.get("end_connections", [])

                # Keep all start/end connections
                important_connections = set(start_conn + end_conn)

                # For any other connections, limit to closest 5
                other_conn = [c for c in connections if c not in important_connections]
                if len(other_conn) > 5:
                    # We could calculate distances here, but for simplicity just keep first 5
                    kept_other = other_conn[:5]
                    opt_gdf.at[idx, "connections"] = (
                        list(important_connections) + kept_other
                    )
            else:
                # For point features, if too many connections (>15), keep only the important ones
                if len(connections) > 15:
                    # Keep connections with a high degree as they're likely important infrastructure
                    important_connections = []
                    for conn in connections:
                        if conn in G:
                            # Keep connections that are well connected themselves
                            if G.degree(conn) >= 3:
                                important_connections.append(conn)

                    # If we've reduced enough, use those; otherwise just trim to 10
                    if (
                        len(important_connections) >= 5
                        and len(important_connections) <= 10
                    ):
                        opt_gdf.at[idx, "connections"] = important_connections
                    else:
                        opt_gdf.at[idx, "connections"] = connections[:10]

        optimized_layers[layer_key] = opt_gdf

    return optimized_layers
