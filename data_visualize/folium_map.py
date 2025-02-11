# folium_map.py

import os
import json
import folium
from folium.plugins import MarkerCluster
import time

from config import csv_files, PROCESSED_DIR, FOLIUM_MAP_OUTPUT


# TODO #4 Refactor Write proper docstrings for the function
def create_folium_map():
    """
    Loads each processed GeoJSON file from the new folder structure:
    PROCESSED_DIR /{layer_key}.geojson,
    extracts the coordinates (EPSG:4326) for display, and adds features to a Folium map.

    Point features are added via a MarkerCluster and LineString features as PolyLines.
    Detailed popups (with tooltips) are added for each feature.
    """
    time_start = time.time()
    m = folium.Map(location=[46.603354, 1.888334], zoom_start=6)

    input_folder = os.path.join(PROCESSED_DIR)
    if not os.path.exists(input_folder):
        print(f"Input folder {input_folder} not found.")
        return

    for filename in os.listdir(input_folder):
        if not filename.endswith(".geojson"):
            continue

        layer_key = filename.split(".")[0]
        config = csv_files.get(layer_key)
        if config is None:
            print(
                f"Warning: No configuration found for layer key '{layer_key}'. Skipping file {filename}."
            )
            continue

        print(f"Processing layer: {layer_key}")
        fg = folium.FeatureGroup(name=config["layer_name"], show=False)
        marker_cluster = MarkerCluster(name=f"{config['layer_name']} (Points)").add_to(
            fg
        )

        file_path = os.path.join(input_folder, filename)
        with open(file_path, "r") as f:
            geojson_data = json.load(f)

        features = geojson_data.get("features", [])
        print(f"  Processing file: {filename} with {len(features)} features.")

        for feature in features:
            geom = feature.get("geometry", {})
            props = feature.get("properties", {})

            popup_content = f"""
            <b>Layer:</b> {config['layer_name']}<br>
            <b>File:</b> {filename}<br>
            <b>Commune:</b> {props.get('nom_commune', 'N/A')}<br>
            <b>Code Commune:</b> {props.get('code_commune', 'N/A')}<br>
            <b>ID:</b> {props.get('id', 'N/A')}<br>
            <b>Type:</b> {props.get('type', 'N/A')}<br>
            <b>Connections:</b> {", ".join(props.get('connections', []))}<br>
            <b>Start Connections:</b> {", ".join(props.get('start_connections', []))}<br>
            <b>End Connections:</b> {", ".join(props.get('end_connections', []))}<br>
            <b>Connection Count:</b> {len(props.get('connections', []))}<br>
            """

            geom_type = geom.get("type", "Unknown")
            coords = geom.get("coordinates", [])
            if geom_type == "Point":
                if isinstance(coords, list) and len(coords) >= 2:
                    folium.Marker(
                        location=[coords[1], coords[0]],
                        popup=folium.Popup(popup_content, max_width=300),
                        tooltip=folium.Tooltip(f"{config['layer_name']} ({filename})"),
                        icon=folium.Icon(color=config.get("color", "blue")),
                    ).add_to(marker_cluster)
            elif geom_type == "LineString":
                if isinstance(coords, list):
                    line_coords = [[pt[1], pt[0]] for pt in coords]
                    folium.PolyLine(
                        locations=line_coords,
                        popup=folium.Popup(popup_content, max_width=300),
                        tooltip=folium.Tooltip(
                            f"{config['layer_name']} ({filename.split('.')[0]}) {props.get('id', 'N/A').split('_')[-1]}"
                        ),
                        color=config.get("color", "blue"),
                        weight=3,
                    ).add_to(fg)
            else:
                pass

        m.add_child(fg)

    folium.LayerControl(collapsed=False).add_to(m)
    m.save(FOLIUM_MAP_OUTPUT)
    time_end = time.time()
    print("Folium map saved as:", FOLIUM_MAP_OUTPUT)
    print("Time taken:", time_end - time_start, "seconds")
    print(
        f"Final file size: {os.path.getsize(FOLIUM_MAP_OUTPUT) / (1024 * 1024):.2f} MB"
    )
