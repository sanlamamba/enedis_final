from tqdm import tqdm
from shapely.strtree import STRtree

from config import csv_files, PROCESSED_DIR, TARGET_CRS
from loader import load_csv_to_gdf
from saver import save_layers_manual as save_layers


def main():
    # =============================================================================
    # LOAD ALL LAYERS AND BUILD SPATIAL INDEXES
    # =============================================================================
    print("Loading layers into memory...")
    layers = {}
    # FIXME Refactor : Remove Unused StrTree
    for layer_key, config in csv_files.items():
        print(f"  Loading layer: {layer_key}")
        gdf = load_csv_to_gdf(layer_key, config)
        tree = STRtree(list(gdf.geometry))
        layers[layer_key] = {"gdf": gdf, "tree": tree}

    # =============================================================================
    # SAVE THE RESULTS WITH DUAL COORDINATES
    # =============================================================================
    print("Saving updated layers as GeoJSON files...")
    save_layers(layers, TARGET_CRS)


if __name__ == "__main__":
    main()
