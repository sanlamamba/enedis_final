import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from config import DATA_DIR, TARGET_CRS


def load_csv_to_gdf(layer_key: str, config: dict) -> gpd.GeoDataFrame:
    """
    Load a CSV file into a GeoDataFrame by converting the 'geo_shape' column
    (which contains JSON strings) into Shapely geometry objects and reprojecting
    the data from EPSG:4326 to the specified TARGET_CRS.

    Parameters:
        layer_key (str): Identifier for the layer; used to generate unique feature IDs.
        config (dict): Configuration for the layer with at least a "file_path" key.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame with the following columns:
            - 'geo_shape': The original geometry JSON string.
            - 'code_commune': A column from the CSV.
            - 'geometry': The parsed and reprojected Shapely geometry.
            - 'id': A unique ID for each feature in the format "{layer_key}_{i}".
    """
    file_path = os.path.join(DATA_DIR, config["file_path"])
    df = pd.read_csv(file_path, delimiter=";", encoding="utf-8", engine="python")

    geometries = []
    keys_to_keep = ["geo_shape", "code_commune"]
    for geojson_str in df["geo_shape"]:
        try:
            geo_dict = json.loads(geojson_str)
            geometries.append(shape(geo_dict))
        except Exception:
            geometries.append(None)
    df["geometry"] = geometries

    df = df[df["geometry"].notnull()].copy()

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").to_crs(TARGET_CRS)

    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]

    gdf = gdf[keys_to_keep + ["geometry", "id"]]

    return gdf
