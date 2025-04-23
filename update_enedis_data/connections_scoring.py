"""
Scoring algorithms for evaluating potential connections.
"""

from typing import Dict, List, Set, Optional, Any
from shapely.geometry import Point
import geopandas as gpd


def calculate_connection_score(
    source: Point,
    target_geom,
    target_layer: str,
    target_distance: float,
    max_radius: float,
    priority_connections: Optional[Dict[str, Dict[str, Any]]],
    connection_settings: Dict[str, Any],
) -> float:
    """
    Calculate a connection score between source point and target geometry.

    Higher score means better connection candidate.

    Args:
        source: Source point
        target_geom: Target geometry
        target_layer: Layer of target geometry
        target_distance: Distance between source and target
        max_radius: Maximum connection radius
        priority_connections: Priority configuration dict
        connection_settings: Connection settings

    Returns:
        Score value (higher is better)
    """
    distance_score = 1.0 - (target_distance / max_radius)

    direction_score = 0.5
    if target_geom.geom_type == "LineString":
        start_point = Point(target_geom.coords[0])
        end_point = Point(target_geom.coords[-1])

        endpoint_dist = min(source.distance(start_point), source.distance(end_point))
        norm_endpoint_factor = (
            endpoint_dist / target_distance if target_distance > 0 else 1.0
        )
        direction_score = 1.0 - min(norm_endpoint_factor, 1.0)

    type_compatibility_score = 0.5
    if priority_connections and target_layer in priority_connections:
        priority = priority_connections[target_layer].get("priority", 5)
        type_compatibility_score = 1.0 - ((priority - 1) / 5)

    weights = connection_settings["connection_score_weights"]
    final_score = (
        weights["distance"] * distance_score
        + weights["direction"] * direction_score
        + weights["type_compatibility"] * type_compatibility_score
    )

    return final_score


def select_connection_candidates(
    endpoint: Point,
    all_features_proj: gpd.GeoDataFrame,
    spatial_index,
    base_radius: float,
    exclude_list: Optional[List[str]],
    priority_connections: Optional[Dict[str, Dict[str, Any]]],
    mono_connection: bool,
    solo_dict: Optional[Dict[str, Dict[str, Any]]],
    connection_settings: Dict[str, Any],
) -> Set[str]:
    """
    Select candidate connection IDs for a given point with improved selection logic.

    Args:
        endpoint: Point or geometric object serving as reference.
        all_features_proj: Complete set of entities, projected in EPSG:3857.
        spatial_index: Spatial index built on all_features_proj to accelerate spatial queries.
        base_radius: Radius for creating buffer around endpoint.
        exclude_list: List of layer names to exclude from search.
        priority_connections: Dictionary of priorities and radii for certain layers.
        mono_connection: If True, selects a single connection per endpoint based on priority.
        solo_dict: Optional. Dictionary indicating special rules for restricting connections.
        connection_settings: Additional settings for connection logic

    Returns:
        Set of identifiers (strings) for candidates selected as connections.
    """
    if connection_settings.get("adaptive_radius", False):
        initial_buffer = endpoint.buffer(base_radius * 2)
        nearby_count = len(list(spatial_index.intersection(initial_buffer.bounds)))

        if nearby_count > 20:
            adjusted_radius = base_radius * 0.7
        elif nearby_count < 5:
            adjusted_radius = base_radius * 1.5
        else:
            adjusted_radius = base_radius
    else:
        adjusted_radius = base_radius

    final_radius = adjusted_radius * connection_settings.get(
        "default_distance_multiplier", 1.0
    )

    buf = endpoint.buffer(final_radius)

    possible_idx = list(spatial_index.intersection(buf.bounds))

    if not possible_idx:
        return set()

    possible = all_features_proj.iloc[possible_idx]

    matches = possible[possible.intersects(buf)]

    if exclude_list and not matches.empty:
        matches = matches[~matches["source_layer"].isin(exclude_list)]

    if matches.empty:
        return set()

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
                valid = valid.sort_values(by=["distance"])
                return {valid.iloc[0]["id"]}

    if mono_connection and priority_connections and not matches.empty:
        candidates = matches[
            matches["source_layer"].isin(priority_connections.keys())
        ].copy()
        if not candidates.empty:
            candidates["distance"] = candidates.geometry.distance(endpoint)

            valid = candidates[
                candidates.apply(
                    lambda row: row["distance"]
                    <= priority_connections[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]

            if not valid.empty:
                valid["score"] = valid.apply(
                    lambda row: calculate_connection_score(
                        endpoint,
                        row.geometry,
                        row["source_layer"],
                        row["distance"],
                        priority_connections[row["source_layer"]]["radius"],
                        priority_connections,
                        connection_settings,
                    ),
                    axis=1,
                )

                valid = valid.sort_values(by=["score"], ascending=False)
                return {valid.iloc[0]["id"]}

    if connection_settings.get("prefer_closest", True):
        matches_with_scores = matches.copy()
        matches_with_scores["distance"] = matches_with_scores.geometry.distance(
            endpoint
        )

        matches_with_scores["score"] = matches_with_scores.apply(
            lambda row: calculate_connection_score(
                endpoint,
                row.geometry,
                row["source_layer"],
                row["distance"],
                final_radius,
                priority_connections,
                connection_settings,
            ),
            axis=1,
        )

        max_connections = connection_settings.get("max_connections_per_entity", 10)
        top_matches = matches_with_scores.sort_values(
            by=["score"], ascending=False
        ).head(max_connections)
        return set(top_matches["id"].tolist())

    return set(matches["id"].tolist())
