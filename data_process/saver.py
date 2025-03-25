import os
import json
from pyproj import Transformer
from config import PROCESSED_DIR
from utils import get_coordinates, convert_coordinates


def save_layers_manual(
    layers, target_crs, combined_filename="data", output_directory=None
):
    """
    Save each layer's GeoDataFrame as an individual GeoJSON file and combine all layers into one file.
    Coordinates are transformed to EPSG:4326 and unnecessary properties are removed.

    Parameters:
        layers (dict): Dictionary of layers with GeoDataFrames.
        target_crs (str): CRS to transform coordinates from.
        combined_filename (str): Base name for the combined GeoJSON file.
        output_directory (str, optional): Directory to save files. Defaults to PROCESSED_DIR.
    """
    if output_directory is None:
        output_directory = PROCESSED_DIR

    transformer = Transformer.from_crs(target_crs, "EPSG:4326", always_xy=True)
    combined_geojson = {"type": "FeatureCollection", "features": []}

    for layer_key, data in layers.items():
        gdf = data["gdf"]
        layer_geojson = {"type": "FeatureCollection", "features": []}

        if "code_commune" in gdf.columns:
            for code_commune, group in gdf.groupby("code_commune"):
                print(f"Processing {layer_key} - code_commune: {code_commune}")
                geojson_dict = json.loads(group.to_json())
                process_features(geojson_dict, transformer)
                layer_geojson["features"].extend(geojson_dict.get("features", []))
        else:
            print(f"Processing {layer_key}")
            geojson_dict = json.loads(gdf.to_json())
            process_features(geojson_dict, transformer)
            layer_geojson["features"].extend(geojson_dict.get("features", []))

        individual_output_path = os.path.join(output_directory, f"{layer_key}.geojson")
        with open(individual_output_path, "w") as f:
            json.dump(layer_geojson, f, indent=2)
        print(
            f"{len(layer_geojson['features'])} features saved to {individual_output_path}"
        )

        combined_geojson["features"].extend(layer_geojson["features"])

    combined_output_path = os.path.join(
        output_directory, f"{combined_filename}.geojson"
    )
    with open(combined_output_path, "w") as f:
        json.dump(combined_geojson, f, indent=2)
    print(f"Combined GeoJSON saved to {combined_output_path}")


def process_features(geojson_dict, transformer):
    """
    Transform feature coordinates to EPSG:4326 and remove unwanted properties.

    Parameters:
        geojson_dict (dict): A GeoJSON FeatureCollection.
        transformer (pyproj.Transformer): Transformer for coordinate conversion.
    """
    geojson_dict.pop("name", None)
    geojson_dict.pop("crs", None)

    for feature in geojson_dict.get("features", []):
        geom_dict = feature.get("geometry")
        if geom_dict:
            feature["geometry"]["coordinates"] = get_coordinates(geom_dict, transformer)
            feature["properties"].pop("geo_shape", None)
            feature["properties"]["coordinates"] = convert_coordinates(geom_dict)
