"""
Core functionality for spatial connection calculation.
"""

import os
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Set, Tuple, Optional, Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from utils import timed
from connections_scoring import calculate_connection_score, select_connection_candidates
from connections_config import CONNECTION_SETTINGS
from config import LAYERS_CONFIG

# Shared variables for worker processes
_global_all_features_proj = None
_global_spatial_index = None
_global_base_radius = None
_global_exclude_list = None
_global_priority_connections = None
_global_mono_connection = None
_global_connection_settings = None


def init_worker(
    all_features_proj,
    base_radius,
    exclude_list,
    priority_connections,
    mono_connection,
    connection_settings,
):
    """
    Initialize global variables in each worker process.
    """
    global _global_all_features_proj, _global_spatial_index, _global_base_radius
    global _global_exclude_list, _global_priority_connections, _global_mono_connection
    global _global_connection_settings

    _global_all_features_proj = all_features_proj
    _global_spatial_index = all_features_proj.sindex
    _global_base_radius = base_radius
    _global_exclude_list = exclude_list
    _global_priority_connections = priority_connections
    _global_mono_connection = mono_connection
    _global_connection_settings = connection_settings


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
    connection_settings = _global_connection_settings

    geom = feature.geometry
    feature_id = feature["id"]

    # Special handling for LineString (find connections at endpoints)
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
            connection_settings,
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
            connection_settings,
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
            connection_settings=connection_settings,
        )

        # Remove self-connections
        candidate_ids.discard(feature_id)

        # If minimum connections setting is enabled and we have too few connections
        if connection_settings.get("min_connections", 1) > len(
            candidate_ids
        ) and not connection_settings.get("ignore_isolated_entities", False):
            # Try with a slightly larger radius to find at least some connections
            larger_radius = base_radius * 1.5
            additional_candidates = select_connection_candidates(
                endpoint,
                all_features_proj,
                spatial_index,
                larger_radius,
                exclude_list,
                priority_connections,
                mono_connection,
                solo_dict=None,
                connection_settings=connection_settings,
            )
            additional_candidates.discard(feature_id)

            # Combine with original candidates
            candidate_ids = candidate_ids.union(additional_candidates)

            # Limit to top candidates if we found too many
            if len(candidate_ids) > connection_settings.get(
                "max_connections_per_entity", 10
            ):
                # Calculate scores for all candidates
                candidates_df = all_features_proj[
                    all_features_proj["id"].isin(candidate_ids)
                ].copy()
                candidates_df["distance"] = candidates_df.geometry.distance(endpoint)
                candidates_df["score"] = candidates_df.apply(
                    lambda row: calculate_connection_score(
                        endpoint,
                        row.geometry,
                        row["source_layer"],
                        row["distance"],
                        larger_radius,
                        priority_connections,
                        connection_settings,
                    ),
                    axis=1,
                )

                # Sort by score and limit
                top_candidates = candidates_df.sort_values(
                    by=["score"], ascending=False
                ).head(connection_settings.get("max_connections_per_entity", 10))

                candidate_ids = set(top_candidates["id"].tolist())

        return list(candidate_ids), [], []


@timed
def find_connections(
    gdf: gpd.GeoDataFrame,
    all_features: gpd.GeoDataFrame,
    base_radius: float,
    exclude_list: Optional[List[str]] = None,
    priority_connections: Optional[Dict[str, Dict[str, Any]]] = None,
    mono_connection_per_endpoint: bool = False,
    connection_settings: Optional[Dict[str, Any]] = None,
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
        connection_settings: Additional connection settings

    Returns:
        The original GeoDataFrame with three added columns:
            - "connections": list of connection IDs (union of start and end connections)
            - "start_connections": list of connections at start point (for LineStrings)
            - "end_connections": list of connections at end point (for LineStrings)
    """
    if exclude_list is None:
        exclude_list = []

    if connection_settings is None:
        connection_settings = CONNECTION_SETTINGS

    # Project dataframes to a metric CRS for accurate spatial analysis
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    # Ensure spatial index exists and is efficient
    if not hasattr(all_features_proj, "sindex") or all_features_proj.sindex is None:
        all_features_proj.sindex

    # Determine optimal chunk size based on dataframe size
    chunk_size = max(1, min(1000, len(gdf_proj) // (os.cpu_count() * 2)))

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
            connection_settings,
        ),
    ) as executor:
        # Use list to materialize results
        results = list(
            executor.map(
                process_feature_worker, list(gdf_proj.iterrows()), chunksize=chunk_size
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
