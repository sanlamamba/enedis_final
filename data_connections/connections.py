import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from config import csv_files
from concurrent.futures import ProcessPoolExecutor

# Global variables to be set in each worker process
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
    Worker initializer: set global variables so that each worker can access
    the projected candidate features, spatial index, and configuration settings.
    """
    global _global_all_features_proj, _global_spatial_index
    global _global_base_radius, _global_exclude_list, _global_priority_connections, _global_mono_connection_per_endpoint

    _global_all_features_proj = all_features_proj
    _global_spatial_index = _global_all_features_proj.sindex
    _global_base_radius = base_radius
    _global_exclude_list = exclude_list
    _global_priority_connections = priority_connections
    _global_mono_connection_per_endpoint = mono_connection_per_endpoint


def process_feature_worker(item):
    """
    Process a single feature (row) and return its connection lists.
    This function replicates the logic of the original loop.
    """
    idx, feature = item
    all_features_proj = _global_all_features_proj
    spatial_index = _global_spatial_index
    base_radius = _global_base_radius
    exclude_list = _global_exclude_list
    priority_connections = _global_priority_connections
    mono_connection_per_endpoint = _global_mono_connection_per_endpoint

    geom = feature.geometry

    if geom.geom_type == "LineString":
        start_point = Point(geom.coords[0])
        end_point = Point(geom.coords[-1])
        config = csv_files.get(feature["source_layer"], {})
        solo_dict = config.get("solo_connection_if", None)

        # --- Process Start Endpoint ---
        buf_start = start_point.buffer(base_radius)
        start_possible_idx = list(spatial_index.intersection(buf_start.bounds))
        start_possible = all_features_proj.iloc[start_possible_idx]
        start_matches = start_possible[start_possible.intersects(buf_start)]
        if exclude_list:
            start_matches = start_matches[
                ~start_matches["source_layer"].isin(exclude_list)
            ]

        if solo_dict is not None:
            solo_candidates = start_matches[
                start_matches["source_layer"].isin(solo_dict.keys())
            ].copy()
            solo_candidates["distance"] = solo_candidates.geometry.distance(start_point)
            valid_solo = solo_candidates[
                solo_candidates.apply(
                    lambda row: row["distance"]
                    <= solo_dict[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]
            if not valid_solo.empty:
                valid_solo = valid_solo.sort_values(by="distance")
                start_ids = {valid_solo.iloc[0]["id"]}
            else:
                if mono_connection_per_endpoint and priority_connections:
                    candidates = start_matches[
                        start_matches["source_layer"].isin(priority_connections.keys())
                    ].copy()
                    if not candidates.empty:
                        candidates["distance"] = candidates.geometry.distance(
                            start_point
                        )
                        valid_candidates = candidates[
                            candidates.apply(
                                lambda row: row["distance"]
                                <= priority_connections[row["source_layer"]]["radius"],
                                axis=1,
                            )
                        ]
                        if not valid_candidates.empty:
                            valid_candidates["priority"] = valid_candidates[
                                "source_layer"
                            ].map(lambda s: priority_connections[s]["priority"])
                            valid_candidates = valid_candidates.sort_values(
                                by=["priority", "distance"]
                            )
                            start_ids = {valid_candidates.iloc[0]["id"]}
                        else:
                            start_ids = set()
                    else:
                        start_ids = set()
                else:
                    start_ids = set(start_matches["id"].tolist())
        else:
            if mono_connection_per_endpoint and priority_connections:
                candidates = start_matches[
                    start_matches["source_layer"].isin(priority_connections.keys())
                ].copy()
                if not candidates.empty:
                    candidates["distance"] = candidates.geometry.distance(start_point)
                    valid_candidates = candidates[
                        candidates.apply(
                            lambda row: row["distance"]
                            <= priority_connections[row["source_layer"]]["radius"],
                            axis=1,
                        )
                    ]
                    if not valid_candidates.empty:
                        valid_candidates["priority"] = valid_candidates[
                            "source_layer"
                        ].map(lambda s: priority_connections[s]["priority"])
                        valid_candidates = valid_candidates.sort_values(
                            by=["priority", "distance"]
                        )
                        start_ids = {valid_candidates.iloc[0]["id"]}
                    else:
                        start_ids = set()
                else:
                    start_ids = set(start_matches["id"].tolist())
            else:
                start_ids = set(start_matches["id"].tolist())

        if feature["id"] in start_ids:
            start_ids.remove(feature["id"])

        # --- Process End Endpoint ---
        buf_end = end_point.buffer(base_radius)
        end_possible_idx = list(spatial_index.intersection(buf_end.bounds))
        end_possible = all_features_proj.iloc[end_possible_idx]
        end_matches = end_possible[end_possible.intersects(buf_end)]
        if exclude_list:
            end_matches = end_matches[~end_matches["source_layer"].isin(exclude_list)]

        if solo_dict is not None:
            solo_candidates = end_matches[
                end_matches["source_layer"].isin(solo_dict.keys())
            ].copy()
            solo_candidates["distance"] = solo_candidates.geometry.distance(end_point)
            valid_solo = solo_candidates[
                solo_candidates.apply(
                    lambda row: row["distance"]
                    <= solo_dict[row["source_layer"]]["radius"],
                    axis=1,
                )
            ]
            if not valid_solo.empty:
                valid_solo = valid_solo.sort_values(by="distance")
                end_ids = {valid_solo.iloc[0]["id"]}
            else:
                if mono_connection_per_endpoint and priority_connections:
                    candidates = end_matches[
                        end_matches["source_layer"].isin(priority_connections.keys())
                    ].copy()
                    if not candidates.empty:
                        candidates["distance"] = candidates.geometry.distance(end_point)
                        valid_candidates = candidates[
                            candidates.apply(
                                lambda row: row["distance"]
                                <= priority_connections[row["source_layer"]]["radius"],
                                axis=1,
                            )
                        ]
                        if not valid_candidates.empty:
                            valid_candidates["priority"] = valid_candidates[
                                "source_layer"
                            ].map(lambda s: priority_connections[s]["priority"])
                            valid_candidates = valid_candidates.sort_values(
                                by=["priority", "distance"]
                            )
                            end_ids = {valid_candidates.iloc[0]["id"]}
                        else:
                            end_ids = set()
                    else:
                        end_ids = set()
                else:
                    end_ids = set(end_matches["id"].tolist())
        else:
            if mono_connection_per_endpoint and priority_connections:
                candidates = end_matches[
                    end_matches["source_layer"].isin(priority_connections.keys())
                ].copy()
                if not candidates.empty:
                    candidates["distance"] = candidates.geometry.distance(end_point)
                    valid_candidates = candidates[
                        candidates.apply(
                            lambda row: row["distance"]
                            <= priority_connections[row["source_layer"]]["radius"],
                            axis=1,
                        )
                    ]
                    if not valid_candidates.empty:
                        valid_candidates["priority"] = valid_candidates[
                            "source_layer"
                        ].map(lambda s: priority_connections[s]["priority"])
                        valid_candidates = valid_candidates.sort_values(
                            by=["priority", "distance"]
                        )
                        end_ids = {valid_candidates.iloc[0]["id"]}
                    else:
                        end_ids = set()
                else:
                    end_ids = set(end_matches["id"].tolist())
            else:
                end_ids = set(end_matches["id"].tolist())

        if feature["id"] in end_ids:
            end_ids.remove(feature["id"])

        union_ids = start_ids.union(end_ids)
        return list(union_ids), list(start_ids), list(end_ids)
    else:
        if geom.geom_type == "Point":
            endpoints = [geom]
        else:
            endpoints = [geom.centroid]
        candidate_ids = set()
        for endpoint in endpoints:
            buf = endpoint.buffer(base_radius)
            possible_idx = list(spatial_index.intersection(buf.bounds))
            possible = all_features_proj.iloc[possible_idx]
            matches = possible[possible.intersects(buf)]
            if exclude_list:
                matches = matches[~matches["source_layer"].isin(exclude_list)]
            candidate_ids.update(matches["id"].tolist())
        if feature["id"] in candidate_ids:
            candidate_ids.remove(feature["id"])
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
    For each feature in the GeoDataFrame, find nearby features within a given base_radius.
    The logic remains unchanged from the original implementation.
    """
    if exclude_list is None:
        exclude_list = []

    # Project the GeoDataFrames to a metric CRS
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    # Use ProcessPoolExecutor with an initializer so each worker gets the shared data
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

    gdf_final = gdf_proj.to_crs(epsg=4326)
    return gdf_final
