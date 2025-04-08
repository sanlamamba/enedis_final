import os
import logging
import geopandas as gpd

from config import PROCESSED_DIR, csv_files


def create_directory(directory: str) -> None:
    """Create a directory if it does not already exist."""
    os.makedirs(directory, exist_ok=True)


def load_single_geojson(layer: str, config: dict) -> gpd.GeoDataFrame:
    """
    Load a single GeoJSON file as a GeoDataFrame and set its source_layer.

    Parameters:
        layer (str): The name of the layer.
        config (dict): Configuration containing the 'file_path' for the layer.

    Returns:
        GeoDataFrame: The loaded GeoDataFrame with a 'source_layer' column,
                      or None if an error occurs.
    """
    file_path = os.path.join(PROCESSED_DIR, config["file_path"])
    logging.info(f"Loading GeoJSON file for layer '{layer}' from {file_path}...")
    try:
        gdf = gpd.read_file(file_path)
        logging.info(f"{layer} : count of gdf : {len(gdf)}")
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf["source_layer"] = layer
        return gdf
    except Exception as e:
        logging.error(f"Error loading GeoJSON file {file_path}: {e}")
        return None


def load_geojson_files() -> dict:
    """
    Load all GeoJSON files as GeoDataFrames into a dictionary keyed by layer.

    Returns:
        dict: Dictionary of GeoDataFrames for each layer.
    """
    all_layers = {}
    for layer, config in csv_files.items():
        gdf = load_single_geojson(layer, config)
        if gdf is not None:
            all_layers[layer] = gdf
    return all_layers
