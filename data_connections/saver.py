import os
import logging
import pandas as pd
from datetime import datetime
import geopandas as gpd
from loader import create_directory
from config import PROCESSED_DIR


def backup_existing_output(output_dir: str) -> None:
    """
    Backup existing files in the output directory (except the backup folder itself).
    """
    if os.path.exists(output_dir):
        timestamp = datetime.timestamp(datetime.now())
        backup_dir = os.path.join(output_dir, "backups", str(timestamp))
        create_directory(backup_dir)
        for filename in os.listdir(output_dir):
            if filename != "backups":
                src = os.path.join(output_dir, filename)
                dst = os.path.join(backup_dir, filename)
                os.rename(src, dst)
        logging.info(f"Backup made from {output_dir} to {backup_dir}")


def save_layer(gdf: gpd.GeoDataFrame, layer: str, output_dir: str) -> None:
    """
    Save a single GeoDataFrame as a GeoJSON file.
    """
    output_path = os.path.join(output_dir, f"{layer}.geojson")
    try:
        gdf.to_file(output_path, driver="GeoJSON")
        logging.info(f"Saved updated GeoJSON for layer '{layer}' to {output_path}.")
    except Exception as e:
        logging.error(f"Error writing GeoJSON file {output_path}: {e}")


def save_combined_layers(all_gdfs: list, output_dir: str) -> None:
    """
    Combine a list of GeoDataFrames into one and save it as a GeoJSON file.
    """
    try:
        combined_gdf = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True))
        combined_output_path = os.path.join(output_dir, "data.geojson")
        combined_gdf[
            "name"
        ] = "Enedis Geo Data with connections - ver " + datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        combined_gdf.to_file(combined_output_path, driver="GeoJSON")
        print(combined_gdf)
        logging.info(f"Saved combined updated GeoJSON to {combined_output_path}.")
    except Exception as e:
        logging.error(f"Error writing combined GeoJSON file: {e}")


def save_updated_layers(updated_layers: dict) -> None:
    """
    Save each updated layer as a GeoJSON file and combine them into one file.

    Updated layers are saved in the output directory. Before saving, existing
    files are backed up.
    """
    backup_existing_output(PROCESSED_DIR)
    create_directory(PROCESSED_DIR)

    all_gdfs = []
    for layer, gdf in updated_layers.items():
        save_layer(gdf, layer, PROCESSED_DIR)
        all_gdfs.append(gdf)

    if all_gdfs:
        save_combined_layers(all_gdfs, PROCESSED_DIR)
