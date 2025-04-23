"""
Reporting functions for connection quality analysis.
"""

import os
import logging
from typing import Dict, Any
import geopandas as gpd
import pandas as pd

from connections_validation import validate_connections


def create_connection_report(
    layers: Dict[str, gpd.GeoDataFrame], output_path: str = None
) -> Dict[str, Any]:
    """
    Create a detailed report on connection quality and statistics.

    Args:
        layers: Dictionary of GeoDataFrames with connections
        output_path: Optional path to save the report as JSON

    Returns:
        Dictionary with connection statistics
    """
    import json

    logging.info("Generating connection quality report...")

    report = {"summary": {}, "layers": {}, "network": {}, "issues": {}}

    total_entities = sum(len(gdf) for gdf in layers.values())
    total_connections = sum(
        gdf["connections"].apply(len).sum()
        for gdf in layers.values()
        if "connections" in gdf.columns
    )

    report["summary"] = {
        "total_entities": total_entities,
        "total_connections": total_connections,
        "avg_connections_per_entity": (
            total_connections / total_entities if total_entities > 0 else 0
        ),
        "timestamp": pd.Timestamp.now().isoformat(),
    }

    for layer_key, gdf in layers.items():
        if "connections" in gdf.columns:
            conn_counts = gdf["connections"].apply(len)

            layer_stats = {
                "entity_count": len(gdf),
                "connection_count": conn_counts.sum(),
                "avg_connections": conn_counts.mean(),
                "max_connections": conn_counts.max(),
                "min_connections": conn_counts.min(),
                "isolated_entities": (conn_counts == 0).sum(),
                "percent_isolated": (conn_counts == 0).mean() * 100,
                "highly_connected": (conn_counts > 5).sum(),
            }

            report["layers"][layer_key] = layer_stats

    import networkx as nx

    G = nx.Graph()

    for layer_key, gdf in layers.items():
        if "connections" in gdf.columns:
            for idx, row in gdf.iterrows():
                node_id = row["id"]
                for conn_id in row["connections"]:
                    G.add_edge(node_id, conn_id)

    if len(G.nodes()) > 0:
        connected_components = list(nx.connected_components(G))
        largest_component = max(connected_components, key=len)

        report["network"] = {
            "node_count": len(G.nodes()),
            "edge_count": len(G.edges()),
            "density": nx.density(G),
            "connected_components": len(connected_components),
            "largest_component_size": len(largest_component),
            "largest_component_pct": (
                len(largest_component) / len(G.nodes()) * 100
                if len(G.nodes()) > 0
                else 0
            ),
        }

        largest_subgraph = G.subgraph(largest_component)
        try:
            report["network"]["avg_path_length"] = nx.average_shortest_path_length(
                largest_subgraph
            )
        except:
            report["network"]["avg_path_length"] = None

    validation_results = validate_connections(layers)
    report["issues"] = {
        layer_key: issues for layer_key, issues in validation_results.items() if issues
    }

    if output_path:
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(report, f, indent=2)
            logging.info(f"Connection report saved to {output_path}")
        except Exception as e:
            logging.error(f"Error saving connection report: {e}")

    return report
