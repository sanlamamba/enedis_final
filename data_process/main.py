from config import csv_files, TARGET_CRS
from loader import load_csv_to_gdf
from saver import save_layers_manual as save_layers


def main():
    """Load layers from CSV files and save them as GeoJSON files."""
    print("Loading layers into memory...")
    layers = {}
    for layer_key, config in csv_files.items():
        print(f"  Loading layer: {layer_key}")
        gdf = load_csv_to_gdf(layer_key, config)
        layers[layer_key] = {"gdf": gdf}

    print("Saving updated layers as GeoJSON files...")
    save_layers(layers, TARGET_CRS)


if __name__ == "__main__":
    main()
