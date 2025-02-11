import os
import logging

from loader import create_directory
from config import PROCESSED_DIR


def save_updated_layers(updated_layers):
    """
    Save each updated layer (GeoDataFrame) into a GeoJSON file.
    Files are saved in a subdirectory (PROCESSED_DIR + "_con").
    """
    output_dir = PROCESSED_DIR + "_con"
    create_directory(output_dir)
    for layer, gdf in updated_layers.items():
        output_path = os.path.join(output_dir, f"{layer}.geojson")
        try:
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(f"Saved updated GeoJSON for layer '{layer}' to {output_path}.")
        except Exception as e:
            logging.error(f"Error writing GeoJSON file {output_path}: {e}")
