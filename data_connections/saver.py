import os
import json
from pyproj import Transformer

from utils import get_dual_coordinates


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
                        feature["geometry"]["coordinates"] = get_dual_coordinates(
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
                    feature["geometry"]["coordinates"] = get_dual_coordinates(
                        geom_dict, transformer
                    )
            with open(output_path, "w") as f:
                json.dump(geojson_dict, f)
