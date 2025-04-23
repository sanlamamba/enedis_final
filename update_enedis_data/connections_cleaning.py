"""
Functions for cleaning and removing redundant connections.
"""

import logging
from typing import Dict, List, Set
import geopandas as gpd
import networkx as nx


def clean_connections(
    layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Clean and refine connections to remove redundancies and topological issues.

    Args:
        layers: Dictionary of GeoDataFrames with connections

    Returns:
        Dictionary of GeoDataFrames with cleaned connections
    """
    logging.info("Cleaning connections to remove redundancies and improve topology...")

    cleaned_layers = {k: v.copy() for k, v in layers.items()}

    import networkx as nx

    G = nx.Graph()

    for layer_key, gdf in cleaned_layers.items():
        for idx, row in gdf.iterrows():
            node_id = row["id"]
            layer_type = layer_key
            G.add_node(node_id, layer=layer_type, geometry=row.geometry)

            for conn_id in row.get("connections", []):
                G.add_edge(node_id, conn_id, weight=1.0)

    redundant_edges = set()

    for node in G.nodes():
        neighbors = list(G.neighbors(node))

        if len(neighbors) < 3:
            continue

        for i, n1 in enumerate(neighbors):
            for n2 in neighbors[i + 1 :]:
                if G.has_edge(n1, n2):
                    node_geom = G.nodes[node].get("geometry")
                    n1_geom = G.nodes[n1].get("geometry")
                    n2_geom = G.nodes[n2].get("geometry")

                    if all([node_geom, n1_geom, n2_geom]):
                        d_node_n1 = node_geom.distance(n1_geom)
                        d_node_n2 = node_geom.distance(n2_geom)
                        d_n1_n2 = n1_geom.distance(n2_geom)

                        # If node->n1->n2 is shorter than node->n2 directly,
                        # mark node->n2 as redundant (keeping the more direct path)
                        if d_node_n1 + d_n1_n2 < d_node_n2 * 1.2:
                            redundant_edges.add((node, n2))
                        elif d_node_n2 + d_n1_n2 < d_node_n1 * 1.2:
                            redundant_edges.add((node, n1))

    for layer_key, gdf in cleaned_layers.items():
        for idx, row in gdf.iterrows():
            if "connections" in gdf.columns:
                filtered_connections = [
                    conn
                    for conn in row["connections"]
                    if (row["id"], conn) not in redundant_edges
                    and (conn, row["id"]) not in redundant_edges
                ]
                gdf.at[idx, "connections"] = filtered_connections

                if (
                    "start_connections" in gdf.columns
                    and "end_connections" in gdf.columns
                ):
                    start_filtered = [
                        conn
                        for conn in row.get("start_connections", [])
                        if (row["id"], conn) not in redundant_edges
                        and (conn, row["id"]) not in redundant_edges
                    ]
                    end_filtered = [
                        conn
                        for conn in row.get("end_connections", [])
                        if (row["id"], conn) not in redundant_edges
                        and (conn, row["id"]) not in redundant_edges
                    ]
                    gdf.at[idx, "start_connections"] = start_filtered
                    gdf.at[idx, "end_connections"] = end_filtered

    logging.info(f"Removed {len(redundant_edges)} redundant connections")
    return cleaned_layers
