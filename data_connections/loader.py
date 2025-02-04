import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from config import DATA_DIR, TARGET_CRS


def load_csv_to_gdf(layer_key, config):
    """
    Load a CSV file and convert its "geo_shape" column (a JSON string)
    into a Shapely geometry. Reprojects from EPSG:4326 to TARGET_CRS.
    """
    file_path = os.path.join(DATA_DIR, config["file_path"])
    df = pd.read_csv(file_path, delimiter=";")

    geometries = []
    for geojson_str in df["geo_shape"]:
        try:
            geo_dict = json.loads(geojson_str)
            geometries.append(shape(geo_dict))
        except Exception:
            geometries.append(None)
    df["geometry"] = geometries
    df = df[df["geometry"].notnull()].copy()

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.to_crs(TARGET_CRS)

    gdf["id"] = [f"{layer_key}_{i}" for i in range(len(gdf))]
    return gdf
