import time
import logging
import geopandas as gpd
import pandas as pd

from loader import load_geojson_files
from connections import find_connections
from saver import save_updated_layers
from config import csv_files, CONNECTION_RADIUS


def main() -> None:
    start_time = time.time()
    logging.info("Loading GeoJSON files into memory...")
    all_layers = load_geojson_files()
    print(f"Count of all_layers: {len(all_layers)}")

    all_features = gpd.GeoDataFrame(
        pd.concat(all_layers.values(), ignore_index=True), crs="EPSG:4326"
    )

    logging.info("Computing connections for each layer...")
    updated_layers = {}
    for layer, gdf in all_layers.items():
        logging.info(f"Processing connections for layer '{layer}'...")
        exclude_list = csv_files[layer].get("exclude_connections", [])
        priority_connections = csv_files[layer].get("priority_connections", None)
        mono_connection = csv_files[layer].get("mono_connection_per_endpoint", False)
        radius = csv_files[layer].get("radius", CONNECTION_RADIUS["mid"])
        updated_gdf = find_connections(
            gdf,
            all_features,
            radius,
            exclude_list,
            priority_connections,
            mono_connection,
        )
        updated_layers[layer] = updated_gdf

    logging.info("Saving updated GeoJSON files...")
    save_updated_layers(updated_layers)
    count_all_features = sum([len(layer) for layer in all_layers.values()])
    elapsed = time.time() - start_time
    logging.info(f"Processed {count_all_features} features in {elapsed:.2f} seconds.")
    logging.info(
        f"Average features processed per second: {count_all_features/elapsed:.2f}"
    )


if __name__ == "__main__":
    main()
