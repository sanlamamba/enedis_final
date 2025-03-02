import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from config import csv_files


def find_connections(
    gdf: gpd.GeoDataFrame,
    all_features: gpd.GeoDataFrame,
    base_radius: float,
    exclude_list: list = None,
    priority_connections: list = None,
    mono_connection_per_endpoint: bool = False,
) -> gpd.GeoDataFrame:
    """
    For each feature in the GeoDataFrame, find nearby features within a given base_radius.

    For non-LineString geometries, a single list of candidate connection IDs is stored in "connections".

    For LineString features, we process the start and end endpoints separately:
        - "start_connections": candidates at the start endpoint.
        - "end_connections"  : candidates at the end endpoint.
        - "connections"      : the union of the above.

    Candidate filtering:
        - Candidates whose 'source_layer' is in exclude_list are removed.
        - If the layer’s config defines a solo_connection_if dictionary (solo_dict),
            then for an endpoint we only consider candidates whose source_layer is in solo_dict
            and further require that the candidate’s distance is ≤ the candidate’s defined radius.
            If any exist, we select only the single closest candidate.
        - Otherwise, if mono_connection_per_endpoint is True and priority_connections is provided,
        we consider only candidates whose source_layer is in the priority_connections dictionary,
        further requiring that the candidate’s distance is ≤ the candidate‑specific radius,
        then choose the candidate with the lowest priority and (if tied) the smallest distance.
      - Otherwise, all candidates within the base_radius are collected.

    Returns a GeoDataFrame (converted back to EPSG:4326) with three new columns:
      - "connections"
      - "start_connections"
      - "end_connections"
    """
    if exclude_list is None:
        exclude_list = []

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
                solo_candidates["distance"] = solo_candidates.geometry.distance(
                    start_point
                )
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
                            start_matches["source_layer"].isin(
                                priority_connections.keys()
                            )
                        ].copy()
                        if not candidates.empty:
                            candidates["distance"] = candidates.geometry.distance(
                                start_point
                            )
                            valid_candidates = candidates[
                                candidates.apply(
                                    lambda row: row["distance"]
                                    <= priority_connections[row["source_layer"]][
                                        "radius"
                                    ],
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
                end_matches = end_matches[
                    ~end_matches["source_layer"].isin(exclude_list)
                ]

            if solo_dict is not None:
                solo_candidates = end_matches[
                    end_matches["source_layer"].isin(solo_dict.keys())
                ].copy()
                solo_candidates["distance"] = solo_candidates.geometry.distance(
                    end_point
                )
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
                            end_matches["source_layer"].isin(
                                priority_connections.keys()
                            )
                        ].copy()
                        if not candidates.empty:
                            candidates["distance"] = candidates.geometry.distance(
                                end_point
                            )
                            valid_candidates = candidates[
                                candidates.apply(
                                    lambda row: row["distance"]
                                    <= priority_connections[row["source_layer"]][
                                        "radius"
                                    ],
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
                buf = endpoint.buffer(base_radius)
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
