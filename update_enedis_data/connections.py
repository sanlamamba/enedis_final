"""
Main connection module that orchestrates the spatial connection calculation process.
"""

import logging
from typing import Dict, List, Any, Optional

import geopandas as gpd
import pandas as pd

from config import LAYERS_CONFIG
from utils import timed
from connections_core import find_connections
from connections_cleaning import clean_connections
from connections_validation import validate_connections
from connections_optimization import optimize_connections

from connections_config import CONNECTION_SETTINGS


@timed
def compute_connections(
    layers: Dict[str, gpd.GeoDataFrame],
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Compute spatial connections between entities with improved logic.

    Args:
        layers: Dictionary of GeoDataFrames for each layer.

    Returns:
        Updated dictionary with GeoDataFrames containing connection information
    """
    if not layers:
        logging.warning("No layers to process for connections")
        return {}

    reprojected_layers = {}
    for layer_key, gdf in layers.items():
        if "source_layer" not in gdf.columns:
            gdf["source_layer"] = layer_key

        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")

        reprojected_layers[layer_key] = gdf

    all_features = gpd.GeoDataFrame(
        pd.concat(list(reprojected_layers.values()), ignore_index=True), crs="EPSG:4326"
    )

    updated_layers = {}
    for layer_key, gdf in layers.items():
        cfg = LAYERS_CONFIG.get(layer_key, {})

        exclude = getattr(cfg, "exclude_connections", [])
        priority = getattr(cfg, "priority_connections", None)
        mono = getattr(cfg, "mono_connection_per_endpoint", False)
        radius = getattr(cfg, "radius", 3)

        layer_connection_settings = CONNECTION_SETTINGS.copy()

        if layer_key.startswith("reseau_"):
            layer_connection_settings["connection_score_weights"] = {
                "distance": 0.6,
                "direction": 0.3,
                "type_compatibility": 0.1,
            }

        if layer_key == "postes_source":
            layer_connection_settings["min_connections"] = 2
            layer_connection_settings["default_distance_multiplier"] = 1.2

        updated_gdf = find_connections(
            gdf,
            all_features,
            radius,
            exclude,
            priority,
            mono,
            connection_settings=layer_connection_settings,
        )
        updated_layers[layer_key] = updated_gdf

    cleaned_layers = clean_connections(updated_layers)

    validation_results = validate_connections(cleaned_layers)
    has_issues = any(len(issues) > 0 for issues in validation_results.values())

    if has_issues:
        logging.warning("Some connection quality issues were found")
    else:
        logging.info("All connections passed validation checks")

    return cleaned_layers
