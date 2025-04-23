"""
Validation functions for checking connection quality.
"""

import logging
from typing import Dict, List
import geopandas as gpd


def validate_connections(layers: Dict[str, gpd.GeoDataFrame]) -> Dict[str, List[str]]:
    """
    Validate the quality and consistency of connections.

    Args:
        layers: Dictionary of GeoDataFrames with connections

    Returns:
        Dictionary mapping layer names to lists of issues
    """
    logging.info("Validating connection quality and consistency...")

    validation_results = {}

    for layer_key, gdf in layers.items():
        layer_issues = []

        if "connections" in gdf.columns:
            isolated_count = (gdf["connections"].apply(len) == 0).sum()
            if isolated_count > 0:
                pct_isolated = (isolated_count / len(gdf)) * 100
                layer_issues.append(
                    f"{isolated_count} isolated entities ({pct_isolated:.1f}% of layer)"
                )

            excessive_count = (gdf["connections"].apply(len) > 50).sum()
            if excessive_count > 0:
                pct_excessive = (excessive_count / len(gdf)) * 100
                layer_issues.append(
                    f"{excessive_count} entities with excessive connections (>{pct_excessive:.1f}% of layer)"
                )

            avg_connections = gdf["connections"].apply(len).mean()
            if avg_connections < 1.0:
                layer_issues.append(
                    f"Low average connection count: {avg_connections:.2f} per entity"
                )

            if len(layers) > 1:
                one_way_count = 0
                for idx, row in gdf.iterrows():
                    for conn_id in row["connections"]:
                        found = False
                        for other_layer, other_gdf in layers.items():
                            if conn_id in other_gdf["id"].values:
                                other_entity = other_gdf[
                                    other_gdf["id"] == conn_id
                                ].iloc[0]
                                if row["id"] in other_entity.get("connections", []):
                                    found = True
                                    break

                        if not found:
                            one_way_count += 1

                if one_way_count > 0:
                    pct_one_way = (
                        one_way_count / gdf["connections"].apply(len).sum()
                    ) * 100
                    layer_issues.append(
                        f"{one_way_count} one-way connections ({pct_one_way:.1f}% of connections)"
                    )

            if "LineString" in gdf.geometry.geom_type.unique():
                no_endpoint_conn = 0
                for idx, row in gdf.iterrows():
                    if row.geometry.geom_type == "LineString":
                        if (
                            len(row.get("start_connections", [])) == 0
                            or len(row.get("end_connections", [])) == 0
                        ):
                            no_endpoint_conn += 1

                if no_endpoint_conn > 0:
                    pct_no_endpoint = (
                        no_endpoint_conn
                        / len(gdf[gdf.geometry.geom_type == "LineString"])
                    ) * 100
                    layer_issues.append(
                        f"{no_endpoint_conn} lines missing endpoint connections ({pct_no_endpoint:.1f}% of lines)"
                    )

        validation_results[layer_key] = layer_issues

        if layer_issues:
            logging.warning(f"Connection issues in layer '{layer_key}':")
            for issue in layer_issues:
                logging.warning(f"  - {issue}")
        else:
            logging.info(f"No connection issues found in layer '{layer_key}'")

    return validation_results
