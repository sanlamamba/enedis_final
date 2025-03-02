import os
import json
from pyproj import Transformer
from utils import get_coordinates, convert_coordinates

manual_output_path = "../output/data.geojson"


# TODO Refactor Document the functions better
def save_layers(layers, processed_dir, target_crs):
    """
    Save the GeoDataFrames stored in `layers` as GeoJSON files.
    Each layer is saved with dual coordinates using a provided CRS transformer.
    """
    transformer = Transformer.from_crs(target_crs, "EPSG:4326", always_xy=True)

    for layer_key, data in layers.items():
        output_dir = os.path.join(processed_dir, layer_key)
        os.makedirs(output_dir, exist_ok=True)
        gdf = data["gdf"]

        if "code_commune" in gdf.columns:
            for code_commune, group in gdf.groupby("code_commune"):
                output_path = os.path.join(output_dir, f"{code_commune}.geojson")
                print(
                    f"  Saving {layer_key} - code_commune: {code_commune} to {output_path}"
                )
                geojson_dict = json.loads(group.to_json())
                geojson_dict.pop("name", None)
                geojson_dict.pop("crs", None)
                for feature in geojson_dict.get("features", []):
                    geom_dict = feature.get("geometry")
                    if geom_dict:
                        feature["geometry"]["coordinates"] = get_coordinates(
                            geom_dict, transformer
                        )
                with open(output_path, "w") as f:
                    json.dump(geojson_dict, f)
        else:
            output_path = os.path.join(output_dir, f"{layer_key}.geojson")
            print(f"  Saving {layer_key} to {output_path}")
            geojson_dict = json.loads(gdf.to_json())
            geojson_dict.pop("name", None)
            geojson_dict.pop("crs", None)
            for feature in geojson_dict.get("features", []):
                geom_dict = feature.get("geometry")
                if geom_dict:
                    feature["geometry"]["coordinates"] = get_coordinates(
                        geom_dict, transformer
                    )
            with open(output_path, "w") as f:
                json.dump(geojson_dict, f)


def save_layers_manual(
    layers, target_crs, combined_filename="data", output_directory="../output"
):
    """
    Save all GeoDataFrames stored in `layers` as individual GeoJSON files and one combined file.
    The features from each layer (grouped by `code_commune` if available) are combined into a single FeatureCollection.
    Each feature is updated with dual coordinates using a provided CRS transformer.

    Parameters:
    - layers: Dictionary of layers with GeoDataFrames.
    - target_crs: The CRS to transform coordinates from.
    - output_directory: Directory where the files will be saved.
    - combined_filename: Name of the file to store the combined GeoJSON data.
    """
    transformer = Transformer.from_crs(target_crs, "EPSG:4326", always_xy=True)

    combined_geojson = {"type": "FeatureCollection", "features": []}

    for layer_key, data in layers.items():
        gdf = data["gdf"]
        layer_geojson = {"type": "FeatureCollection", "features": []}

        if "code_commune" in gdf.columns:
            for code_commune, group in gdf.groupby("code_commune"):
                print(f"  Processing {layer_key} - code_commune: {code_commune}")
                geojson_dict = json.loads(group.to_json())
                process_features(geojson_dict, transformer)
                layer_geojson["features"].extend(geojson_dict.get("features", []))
        else:
            print(f"  Processing {layer_key}")
            geojson_dict = json.loads(gdf.to_json())
            process_features(geojson_dict, transformer)
            layer_geojson["features"].extend(geojson_dict.get("features", []))

        individual_output_path = f"{output_directory}/{layer_key}.geojson"
        with open(individual_output_path, "w") as f:
            json.dump(layer_geojson, f, indent=2)

        print(
            f"  {len(layer_geojson['features'])} features saved to {individual_output_path}"
        )

        combined_geojson["features"].extend(layer_geojson["features"])

    combined_output_path = f"{output_directory}/{combined_filename}.geojson"
    with open(combined_output_path, "w") as f:
        json.dump(combined_geojson, f, indent=2)

    print(f"  Combined GeoJSON saved to {combined_output_path}")


def process_features(geojson_dict, transformer):
    """
    Update each feature's coordinates and remove unwanted properties.
    """
    geojson_dict.pop("name", None)
    geojson_dict.pop("crs", None)

    for feature in geojson_dict.get("features", []):
        geom_dict = feature.get("geometry")
        if geom_dict:
            feature["geometry"]["coordinates"] = get_coordinates(geom_dict, transformer)
            feature["properties"].pop("geo_shape", None)
            feature["properties"]["coordinates"] = convert_coordinates(geom_dict)
