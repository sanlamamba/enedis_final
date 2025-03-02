import os
import logging
import geopandas as gpd

from config import PROCESSED_DIR, csv_files


def create_directory(directory: str) -> None:
    """Create a directory if it does not already exist."""
    os.makedirs(directory, exist_ok=True)


def load_geojson_files() -> dict:
    """
    Load all GeoJSON files into a dictionary of GeoDataFrames.
    Each GeoDataFrame is given a 'source_layer' column based on the key.
    """
    all_layers = {}
    for layer, config in csv_files.items():
        file_path = os.path.join(PROCESSED_DIR, config["file_path"])
        logging.info(f"Loading GeoJSON file for layer '{layer}' from {file_path}...")
        try:
            gdf = gpd.read_file(file_path)
            print(f"{layer} : count of gdf : {len(gdf)}")
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            gdf["source_layer"] = layer
            all_layers[layer] = gdf
        except Exception as e:
            logging.error(f"Error loading GeoJSON file {file_path}: {e}")
            continue
    return all_layers
