from tqdm import tqdm
from shapely.strtree import STRtree

from config import csv_files, PROCESSED_DIR, TARGET_CRS
from loader import load_csv_to_gdf
from connections import process_feature_connections_for_feature
from saver import save_layers


def main():
    # =============================================================================
    # LOAD ALL LAYERS AND BUILD SPATIAL INDEXES
    # =============================================================================
    print("Loading layers into memory...")
    layers = {}

    for layer_key, config in csv_files.items():
        print(f"  Loading layer: {layer_key}")
        gdf = load_csv_to_gdf(layer_key, config)
        tree = STRtree(list(gdf.geometry))
        layers[layer_key] = {"gdf": gdf, "tree": tree}

    # =============================================================================
    # PROCESS CONNECTIONS
    # =============================================================================
    print("Processing connections for each layer...")

    for layer_key, config in csv_files.items():
        print(f"  Processing connections for layer: {layer_key}")
        main_gdf = layers[layer_key]["gdf"]

        if config.get("exclude_connections") is None:
            allowed_layer_keys = [k for k in layers.keys() if k != layer_key]
        else:
            allowed_layer_keys = [
                k for k in layers.keys() if k not in config.get("exclude_connections")
            ]

        connections_list = []
        for _, row in tqdm(
            main_gdf.iterrows(), total=len(main_gdf), desc=f"Layer {layer_key}"
        ):
            conn = process_feature_connections_for_feature(
                row, config, allowed_layer_keys, layers
            )
            connections_list.append(conn)
        main_gdf["connections"] = connections_list
        layers[layer_key]["gdf"] = main_gdf

    # =============================================================================
    # SAVE THE RESULTS WITH DUAL COORDINATES
    # =============================================================================
    print("Saving updated layers as GeoJSON files...")
    save_layers(layers, PROCESSED_DIR, TARGET_CRS)


if __name__ == "__main__":
    main()
