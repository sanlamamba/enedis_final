import time
import logging
import geopandas as gpd
import pandas as pd

from loader import load_geojson_files
from connections import find_connections
from saver import save_updated_layers
from config import csv_files, CONNECTION_RADIUS


def load_all_layers():
    """
    Load all GeoJSON files as GeoDataFrames and create a combined GeoDataFrame.

    Returns:
        tuple: A dictionary of GeoDataFrames (keyed by layer name) and a combined GeoDataFrame.
    """
    logging.info("Loading GeoJSON files into memory...")
    all_layers = load_geojson_files()
    logging.info(f"Loaded {len(all_layers)} layers.")
    all_features = gpd.GeoDataFrame(
        pd.concat(all_layers.values(), ignore_index=True), crs="EPSG:4326"
    )
    return all_layers, all_features


def compute_connections_for_layers(
    all_layers: dict, all_features: gpd.GeoDataFrame
) -> dict:
    """
    Compute spatial connections for each layer using the configuration settings.

    Parameters:
        all_layers (dict): Dictionary of GeoDataFrames keyed by layer name.
        all_features (GeoDataFrame): Combined GeoDataFrame of all features.

    Returns:
        dict: Updated GeoDataFrames with connection information for each layer.
    """
    logging.info("Computing connections for each layer...")
    updated_layers = {}
    for layer, gdf in all_layers.items():
        logging.info(f"Processing connections for layer '{layer}'...")
        config = csv_files[layer]
        exclude_list = config.get("exclude_connections", [])
        priority_connections = config.get("priority_connections", None)
        mono_connection = config.get("mono_connection_per_endpoint", False)
        radius = config.get("radius", CONNECTION_RADIUS["mid"])
        updated_gdf = find_connections(
            gdf,
            all_features,
            radius,
            exclude_list,
            priority_connections,
            mono_connection,
        )
        updated_layers[layer] = updated_gdf
    return updated_layers


def log_performance(all_layers: dict, elapsed: float) -> None:
    """
    Log processing performance statistics.

    Parameters:
        all_layers (dict): Dictionary of GeoDataFrames.
        elapsed (float): Total time elapsed.
    """
    total_features = sum(len(layer) for layer in all_layers.values())
    logging.info(f"Processed {total_features} features in {elapsed:.2f} seconds.")
    logging.info(f"Average features processed per second: {total_features/elapsed:.2f}")


def main() -> None:
    start_time = time.time()
    all_layers, all_features = load_all_layers()
    updated_layers = compute_connections_for_layers(all_layers, all_features)
    logging.info("Saving updated GeoJSON files...")
    save_updated_layers(updated_layers)
    elapsed = time.time() - start_time
    log_performance(all_layers, elapsed)


if __name__ == "__main__":
    main()
