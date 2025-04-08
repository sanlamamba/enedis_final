import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from config import csv_files
from concurrent.futures import ProcessPoolExecutor

# Global variables for worker processes
_global_all_features_proj = None
_global_spatial_index = None
_global_base_radius = None
_global_exclude_list = None
_global_priority_connections = None
_global_mono_connection_per_endpoint = None


def init_worker(
    all_features_proj,
    base_radius,
    exclude_list,
    priority_connections,
    mono_connection_per_endpoint,
):
    """
    Initialize global variables for worker processes.
    """
    global _global_all_features_proj, _global_spatial_index
    global _global_base_radius, _global_exclude_list, _global_priority_connections, _global_mono_connection_per_endpoint

    _global_all_features_proj = all_features_proj
    _global_spatial_index = _global_all_features_proj.sindex
    _global_base_radius = base_radius
    _global_exclude_list = exclude_list
    _global_priority_connections = priority_connections
    _global_mono_connection_per_endpoint = mono_connection_per_endpoint


def select_connection_candidates(
    endpoint,
    all_features_proj,
    spatial_index,
    base_radius,
    exclude_list,
    priority_connections,
    mono_connection,
    solo_dict,
):
    """
    For a given endpoint, select candidate feature IDs based on distance, priority,
    and filtering criteria.

    Parameters:
        endpoint: Shapely geometry of the endpoint.
        all_features_proj: GeoDataFrame with all features (projected).
        spatial_index: Spatial index for quick spatial queries.
        base_radius: Buffer distance for connection search.
        exclude_list: List of source_layer names to exclude.
        priority_connections: Priority settings dictionary.
        mono_connection: Boolean flag for single connection per endpoint.
        solo_dict: Optional dict for solo connection criteria.

    Returns:
        set: Candidate feature IDs.
    """
    buf = endpoint.buffer(base_radius)
    possible_idx = list(spatial_index.intersection(buf.bounds))
    possible = all_features_proj.iloc[possible_idx]
    matches = possible[possible.intersects(buf)]
    if exclude_list:
        matches = matches[~matches["source_layer"].isin(exclude_list)]

    # Try solo connection criteria first.
    if solo_dict is not None:
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

    # Next, try priority-based single connection.
    if mono_connection and priority_connections:
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
                valid["priority"] = valid["source_layer"].map(
                    lambda s: priority_connections[s]["priority"]
                )
                valid = valid.sort_values(by=["priority", "distance"])
                return {valid.iloc[0]["id"]}
        return set(matches["id"].tolist())

    # Default: return all candidate IDs.
    return set(matches["id"].tolist())


def process_feature_worker(item):
    """
    Process a single feature and compute its connection IDs.

    For LineString features, connections are computed separately for the start
    and end endpoints. For other geometries, the point or its centroid is used.

    Returns:
        tuple: (union_ids, start_ids, end_ids) as lists.
    """
    idx, feature = item
    all_features_proj = _global_all_features_proj
    spatial_index = _global_spatial_index
    base_radius = _global_base_radius
    exclude_list = _global_exclude_list
    priority_connections = _global_priority_connections
    mono_connection = _global_mono_connection_per_endpoint

    geom = feature.geometry
    feature_id = feature["id"]

    if geom.geom_type == "LineString":
        start_point = Point(geom.coords[0])
        end_point = Point(geom.coords[-1])
        config = csv_files.get(feature["source_layer"], {})
        solo_dict = config.get("solo_connection_if", None)

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
        # Remove self-references.
        start_ids.discard(feature_id)
        end_ids.discard(feature_id)
        union_ids = start_ids.union(end_ids)
        return list(union_ids), list(start_ids), list(end_ids)
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
        candidate_ids.discard(feature_id)
        return list(candidate_ids), [], []


def find_connections(
    gdf: gpd.GeoDataFrame,
    all_features: gpd.GeoDataFrame,
    base_radius: float,
    exclude_list: list = None,
    priority_connections: dict = None,
    mono_connection_per_endpoint: bool = False,
) -> gpd.GeoDataFrame:
    """
    For each feature in the GeoDataFrame, find nearby features within base_radius.
    The result includes a list of connection IDs and separate lists for start and end connections.

    Parameters:
        gdf: GeoDataFrame of features to process.
        all_features: GeoDataFrame of all features (for candidate search).
        base_radius: Buffer distance for connection search.
        exclude_list: List of source_layer names to exclude.
        priority_connections: Dict specifying priority connection settings.
        mono_connection_per_endpoint: Flag for allowing only one connection per endpoint.

    Returns:
        GeoDataFrame: Updated with "connections", "start_connections", and "end_connections" columns.
    """
    if exclude_list is None:
        exclude_list = []

    # Project to a metric CRS for accurate distance calculations.
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    with ProcessPoolExecutor(
        initializer=init_worker,
        initargs=(
            all_features_proj,
            base_radius,
            exclude_list,
            priority_connections,
            mono_connection_per_endpoint,
        ),
    ) as executor:
        results = list(executor.map(process_feature_worker, list(gdf_proj.iterrows())))

    connections, start_connections, end_connections = zip(*results)
    gdf_proj["connections"] = connections
    gdf_proj["start_connections"] = start_connections
    gdf_proj["end_connections"] = end_connections

    return gdf_proj.to_crs(epsg=4326)
