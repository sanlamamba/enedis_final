import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

from config import csv_files


# TODO Refactor break down into smaller chunks
def find_connections(
    gdf,
    all_features,
    radius,
    exclude_list=None,
    priority_connections=None,
    mono_connection_per_endpoint=False,
):
    """
    For each feature in the GeoDataFrame, find nearby features within a given radius.

    Returns a GeoDataFrame with three new columns:
      - "connections"         : Union of all connections (for non-LineString features, it is a single list).
      - "start_connections"   : Connections from the starting endpoint (only for LineStrings).
      - "end_connections"     : Connections from the ending endpoint (only for LineStrings).

    Any candidate whose 'source_layer' is in the exclude_list is filtered out.
    """
    if exclude_list is None:
        exclude_list = []

    # Projection pour un calcul plus préc
    gdf_proj = gdf.to_crs(epsg=3857).copy()
    all_features_proj = all_features.to_crs(epsg=3857).copy()

    spatial_index = all_features_proj.sindex

    connections = []
    start_connections = []
    end_connections = []

    for idx, feature in gdf_proj.iterrows():
        geom = feature.geometry

        if geom.geom_type == "LineString":
            start_point = Point(geom.coords[0])
            end_point = Point(geom.coords[-1])
            config = csv_files.get(feature["source_layer"], {})
            solo_list = config.get("solo_connection_if", None)

            # --- Process Start Endpoint ---
            buf_start = start_point.buffer(radius)
            start_possible_idx = list(spatial_index.intersection(buf_start.bounds))
            start_possible = all_features_proj.iloc[start_possible_idx]
            start_matches = start_possible[start_possible.intersects(buf_start)]
            if exclude_list:
                start_matches = start_matches[
                    ~start_matches["source_layer"].isin(exclude_list)
                ]

            if solo_list is not None:
                solo_candidates = start_matches[
                    start_matches["source_layer"].isin(solo_list)
                ]
                if not solo_candidates.empty:
                    solo_candidates = solo_candidates.copy()
                    solo_candidates["distance"] = solo_candidates.geometry.distance(
                        start_point
                    )
                    solo_candidates_sorted = solo_candidates.sort_values(by="distance")
                    start_ids = {solo_candidates_sorted.iloc[0]["id"]}
                else:
                    if mono_connection_per_endpoint and priority_connections:
                        candidates = start_matches[
                            start_matches["source_layer"].isin(
                                priority_connections.keys()
                            )
                        ]
                        if not candidates.empty:
                            candidates = candidates.copy()
                            candidates["distance"] = candidates.geometry.distance(
                                start_point
                            )
                            candidates["priority"] = candidates["source_layer"].map(
                                priority_connections
                            )
                            candidates_sorted = candidates.sort_values(
                                by=["priority", "distance"]
                            )
                            start_ids = {candidates_sorted.iloc[0]["id"]}
                        else:
                            start_ids = set()
                    else:
                        start_ids = set(start_matches["id"].tolist())
            else:
                if mono_connection_per_endpoint and priority_connections:
                    candidates = start_matches[
                        start_matches["source_layer"].isin(priority_connections.keys())
                    ]
                    if not candidates.empty:
                        candidates = candidates.copy()
                        candidates["distance"] = candidates.geometry.distance(
                            start_point
                        )
                        candidates["priority"] = candidates["source_layer"].map(
                            priority_connections
                        )
                        candidates_sorted = candidates.sort_values(
                            by=["priority", "distance"]
                        )
                        start_ids = {candidates_sorted.iloc[0]["id"]}
                    else:
                        start_ids = set()
                else:
                    start_ids = set(start_matches["id"].tolist())

            if feature["id"] in start_ids:
                start_ids.remove(feature["id"])

            # --- Process End Endpoint ---
            buf_end = end_point.buffer(radius)
            end_possible_idx = list(spatial_index.intersection(buf_end.bounds))
            end_possible = all_features_proj.iloc[end_possible_idx]
            end_matches = end_possible[end_possible.intersects(buf_end)]
            if exclude_list:
                end_matches = end_matches[
                    ~end_matches["source_layer"].isin(exclude_list)
                ]

            if solo_list is not None:
                solo_candidates = end_matches[
                    end_matches["source_layer"].isin(solo_list)
                ]
                if not solo_candidates.empty:
                    solo_candidates = solo_candidates.copy()
                    solo_candidates["distance"] = solo_candidates.geometry.distance(
                        end_point
                    )
                    solo_candidates_sorted = solo_candidates.sort_values(by="distance")
                    end_ids = {solo_candidates_sorted.iloc[0]["id"]}
                else:
                    if mono_connection_per_endpoint and priority_connections:
                        candidates = end_matches[
                            end_matches["source_layer"].isin(
                                priority_connections.keys()
                            )
                        ]
                        if not candidates.empty:
                            candidates = candidates.copy()
                            candidates["distance"] = candidates.geometry.distance(
                                end_point
                            )
                            candidates["priority"] = candidates["source_layer"].map(
                                priority_connections
                            )
                            candidates_sorted = candidates.sort_values(
                                by=["priority", "distance"]
                            )
                            end_ids = {candidates_sorted.iloc[0]["id"]}
                        else:
                            end_ids = set()
                    else:
                        end_ids = set(end_matches["id"].tolist())
            else:
                if mono_connection_per_endpoint and priority_connections:
                    candidates = end_matches[
                        end_matches["source_layer"].isin(priority_connections.keys())
                    ]
                    if not candidates.empty:
                        candidates = candidates.copy()
                        candidates["distance"] = candidates.geometry.distance(end_point)
                        candidates["priority"] = candidates["source_layer"].map(
                            priority_connections
                        )
                        candidates_sorted = candidates.sort_values(
                            by=["priority", "distance"]
                        )
                        end_ids = {candidates_sorted.iloc[0]["id"]}
                    else:
                        end_ids = set()
                else:
                    end_ids = set(end_matches["id"].tolist())

            if feature["id"] in end_ids:
                end_ids.remove(feature["id"])

            union_ids = start_ids.union(end_ids)
            connections.append(list(union_ids))
            start_connections.append(list(start_ids))
            end_connections.append(list(end_ids))
        else:
            if geom.geom_type == "Point":
                endpoints = [geom]
            else:
                endpoints = [geom.centroid]
            candidate_ids = set()
            for endpoint in endpoints:
                buf = endpoint.buffer(radius)
                possible_idx = list(spatial_index.intersection(buf.bounds))
                possible = all_features_proj.iloc[possible_idx]
                matches = possible[possible.intersects(buf)]
                if exclude_list:
                    matches = matches[~matches["source_layer"].isin(exclude_list)]
                candidate_ids.update(matches["id"].tolist())
            if feature["id"] in candidate_ids:
                candidate_ids.remove(feature["id"])
            connections.append(list(candidate_ids))
            start_connections.append([])
            end_connections.append([])

    gdf_proj["connections"] = connections
    gdf_proj["start_connections"] = start_connections
    gdf_proj["end_connections"] = end_connections

    gdf_final = gdf_proj.to_crs(epsg=4326)
    return gdf_final
