"""
Network optimization functions for improving topology.
"""

import logging
from typing import Dict, List
import geopandas as gpd
import networkx as nx
from shapely.geometry import Point

from connections_validation import validate_connections
from connections_cleaning import clean_connections


def optimize_connections(
    layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Optimize the connection network for better topology and visualization.

    Args:
        layers: Dictionary of GeoDataFrames with calculated connections

    Returns:
        Dictionary with optimized connection links
    """
    logging.info("Optimizing connection network topology...")

    optimized_layers = clean_connections(layers)

    import networkx as nx

    G = nx.Graph()

    node_layer_map = {}

    for layer_key, gdf in optimized_layers.items():
        for idx, row in gdf.iterrows():
            node_id = row["id"]
            G.add_node(node_id, layer=layer_key, geometry=row.geometry)
            node_layer_map[node_id] = layer_key

            for conn_id in row.get("connections", []):
                G.add_edge(node_id, conn_id, weight=1.0)

    if len(G.nodes()) > 0:
        components = list(nx.connected_components(G))
        logging.info(f"Network has {len(components)} connected components")

        if len(components) > 1:
            component_nodes = [list(comp) for comp in components]

            for i in range(len(component_nodes)):
                for j in range(i + 1, len(component_nodes)):
                    comp_i_nodes = component_nodes[i]
                    comp_j_nodes = component_nodes[j]

                    min_distance = float("inf")
                    closest_pair = (None, None)

                    for node_i in comp_i_nodes:
                        for node_j in comp_j_nodes:
                            if node_i in G.nodes() and node_j in G.nodes():
                                geom_i = G.nodes[node_i].get("geometry")
                                geom_j = G.nodes[node_j].get("geometry")

                                if geom_i and geom_j:
                                    dist = geom_i.distance(geom_j)
                                    if dist < min_distance:
                                        min_distance = dist
                                        closest_pair = (node_i, node_j)

                    if closest_pair[0] and closest_pair[1] and min_distance < 500:
                        logging.info(
                            f"Adding bridge connection between components: {closest_pair}"
                        )

                        G.add_edge(
                            closest_pair[0], closest_pair[1], weight=0.5, is_bridge=True
                        )

                        node_i, node_j = closest_pair
                        layer_i = node_layer_map[node_i]
                        layer_j = node_layer_map[node_j]

                        gdf_i = optimized_layers[layer_i]
                        gdf_j = optimized_layers[layer_j]

                        idx_i = gdf_i[gdf_i["id"] == node_i].index[0]
                        idx_j = gdf_j[gdf_j["id"] == node_j].index[0]

                        if "connections" in gdf_i.columns:
                            connections_i = gdf_i.at[idx_i, "connections"]
                            if node_j not in connections_i:
                                connections_i.append(node_j)
                                gdf_i.at[idx_i, "connections"] = connections_i

                        if "connections" in gdf_j.columns:
                            connections_j = gdf_j.at[idx_j, "connections"]
                            if node_i not in connections_j:
                                connections_j.append(node_i)
                                gdf_j.at[idx_j, "connections"] = connections_j

    for layer_key, gdf in optimized_layers.items():
        if layer_key.startswith("reseau_"):
            for idx, row in gdf.iterrows():
                if (
                    row.geometry.geom_type == "LineString"
                    and "connections" in gdf.columns
                ):
                    start_connections = row.get("start_connections", [])
                    end_connections = row.get("end_connections", [])

                    if len(start_connections) == 0 or len(end_connections) == 0:
                        line_geom = row.geometry
                        start_point = Point(line_geom.coords[0])
                        end_point = Point(line_geom.coords[-1])

                        for other_key, other_gdf in optimized_layers.items():
                            if other_key != layer_key:

                                if len(start_connections) == 0:
                                    nearest_to_start = None
                                    min_dist = float("inf")

                                    for other_idx, other_row in other_gdf.iterrows():
                                        dist = other_row.geometry.distance(start_point)
                                        if dist < min_dist and dist < 50:
                                            min_dist = dist
                                            nearest_to_start = other_row["id"]

                                    if nearest_to_start:
                                        logging.info(
                                            f"Adding missing start connection for {row['id']} to {nearest_to_start}"
                                        )

                                        connections = row.get("connections", [])
                                        start_connections = row.get(
                                            "start_connections", []
                                        )

                                        if nearest_to_start not in connections:
                                            connections.append(nearest_to_start)
                                        if nearest_to_start not in start_connections:
                                            start_connections.append(nearest_to_start)

                                        gdf.at[idx, "connections"] = connections
                                        gdf.at[idx, "start_connections"] = (
                                            start_connections
                                        )

                                        other_idx = other_gdf[
                                            other_gdf["id"] == nearest_to_start
                                        ].index[0]
                                        other_connections = other_gdf.at[
                                            other_idx, "connections"
                                        ]
                                        if row["id"] not in other_connections:
                                            other_connections.append(row["id"])
                                            other_gdf.at[other_idx, "connections"] = (
                                                other_connections
                                            )

                                if len(end_connections) == 0:
                                    nearest_to_end = None
                                    min_dist = float("inf")

                                    for other_idx, other_row in other_gdf.iterrows():
                                        dist = other_row.geometry.distance(end_point)
                                        if (
                                            dist < min_dist and dist < 50
                                        ):  # Reasonable threshold
                                            min_dist = dist
                                            nearest_to_end = other_row["id"]

                                    if nearest_to_end:
                                        logging.info(
                                            f"Adding missing end connection for {row['id']} to {nearest_to_end}"
                                        )

                                        # Add to connections
                                        connections = row.get("connections", [])
                                        end_connections = row.get("end_connections", [])

                                        if nearest_to_end not in connections:
                                            connections.append(nearest_to_end)
                                        if nearest_to_end not in end_connections:
                                            end_connections.append(nearest_to_end)

                                        gdf.at[idx, "connections"] = connections
                                        gdf.at[idx, "end_connections"] = end_connections

                                        # Add reciprocal connection
                                        other_idx = other_gdf[
                                            other_gdf["id"] == nearest_to_end
                                        ].index[0]
                                        other_connections = other_gdf.at[
                                            other_idx, "connections"
                                        ]
                                        if row["id"] not in other_connections:
                                            other_connections.append(row["id"])
                                            other_gdf.at[other_idx, "connections"] = (
                                                other_connections
                                            )

    validation_results = validate_connections(optimized_layers)
    has_issues = any(len(issues) > 0 for issues in validation_results.values())

    if has_issues:
        logging.warning("Some connection issues remain after optimization:")
        for layer_key, issues in validation_results.items():
            if issues:
                for issue in issues:
                    logging.warning(f"  - {layer_key}: {issue}")
    else:
        logging.info("Network optimization completed successfully with no issues")

    return optimized_layers
