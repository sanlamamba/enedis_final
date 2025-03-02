import os
import logging
import pandas as pd
from loader import create_directory
from config import PROCESSED_DIR
import geopandas as gpd
from datetime import datetime


def save_updated_layers(updated_layers: dict) -> None:
    """
    Save each updated layer (GeoDataFrame) into a GeoJSON file.
    Files are saved in a subdirectory (PROCESSED_DIR + "_con").
    Additionally, all layers are combined and saved as data.geojson.
    """
    if os.path.exists(PROCESSED_DIR):
        timestamp = datetime.timestamp(datetime.now())
        backup_dir = os.path.join(PROCESSED_DIR, "backups", str(timestamp))
        create_directory(backup_dir)
        for file in os.listdir(PROCESSED_DIR):
            if file != "backups":
                os.rename(
                    os.path.join(PROCESSED_DIR, file), os.path.join(backup_dir, file)
                )
        logging.info(f"Backup made from {PROCESSED_DIR} to {backup_dir}")

    output_dir = PROCESSED_DIR
    create_directory(output_dir)

    all_gdfs = []

    for layer, gdf in updated_layers.items():
        output_path = os.path.join(output_dir, f"{layer}.geojson")
        try:
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(f"Saved updated GeoJSON for layer '{layer}' to {output_path}.")
            all_gdfs.append(gdf)
        except Exception as e:
            logging.error(f"Error writing GeoJSON file {output_path}: {e}")

    if all_gdfs:
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
