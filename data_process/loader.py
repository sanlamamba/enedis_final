import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from config import DATA_DIR, TARGET_CRS


# TODO Refactor #1: DOcument better the function
def load_csv_to_gdf(layer_key: str, config: dict) -> gpd.GeoDataFrame:
    """
    Load a CSV file and convert its "geo_shape" column (a JSON string)
    into a Shapely geometry. Reprojects from EPSG:4326 to TARGET_CRS.
    """
    file_path = os.path.join(DATA_DIR, config["file_path"])
    df = pd.read_csv(file_path, delimiter=";", encoding="utf-8", engine="python")

    geometries = []
    keys_to_keep = [
        "geo_shape",
        "code_commune",
    ]
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

    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    # Suppression des colonnes non utilisées
    gdf = gdf[keys_to_keep + ["geometry", "id"]]

    return gdf
