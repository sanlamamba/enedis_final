from shapely.geometry import Point, LineString
from utils import get_endpoints


def process_feature_connections_for_feature(
    main_feat, main_config, allowed_layer_keys, layers
):
    """
    Given one feature (from a main layer) and a list of allowed layer keys, find all features
    from those layers whose geometry (or, for LineStrings, whose endpoints) is within the given radius.
    Returns a list of connected feature ids.
    """
    connections = []
    main_geom = main_feat.geometry
    radius = main_config.get("connection_radius", 0)

    if isinstance(main_geom, Point):
        main_point = main_geom
        buffer_geom = main_point.buffer(radius)
        for layer_key in allowed_layer_keys:
            tree = layers[layer_key]["tree"]
            other_gdf = layers[layer_key]["gdf"]
            candidate_indices = tree.query(buffer_geom)
            for idx in candidate_indices:
                candidate_feat = other_gdf.iloc[idx]
                candidate_geom = candidate_feat.geometry
                if isinstance(candidate_geom, Point):
                    if main_point.distance(candidate_geom) <= radius:
                        connections.append(candidate_feat["id"])
                elif isinstance(candidate_geom, LineString):
                    start, end = get_endpoints(candidate_geom)
                    if start is not None:
                        if (
                            main_point.distance(start) <= radius
                            or main_point.distance(end) <= radius
                        ):
                            connections.append(candidate_feat["id"])

    elif isinstance(main_geom, LineString):
        start, end = get_endpoints(main_geom)
        if start is None:
            return []
        union_buffer = start.buffer(radius).union(end.buffer(radius))
        for layer_key in allowed_layer_keys:
            tree = layers[layer_key]["tree"]
            other_gdf = layers[layer_key]["gdf"]
            candidate_indices = tree.query(union_buffer)
            for idx in candidate_indices:
                candidate_feat = other_gdf.iloc[idx]
                candidate_geom = candidate_feat.geometry
                if isinstance(candidate_geom, Point):
                    if (
                        start.distance(candidate_geom) <= radius
                        or end.distance(candidate_geom) <= radius
                    ):
                        connections.append(candidate_feat["id"])
                elif isinstance(candidate_geom, LineString):
                    cstart, cend = get_endpoints(candidate_geom)
                    if cstart is not None:
                        if (
                            start.distance(cstart) <= radius
                            or start.distance(cend) <= radius
                            or end.distance(cstart) <= radius
                            or end.distance(cend) <= radius
                        ):
                            connections.append(candidate_feat["id"])

    connections = list(set(connections))
    max_conn = main_config.get("max_connections")
    if max_conn is not None and isinstance(main_geom, LineString):
        connections = connections[: max_conn * 2]

    return connections
