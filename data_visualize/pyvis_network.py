import os
import json
import time
from pyvis.network import Network

from config import csv_files, PROCESSED_DIR, PYVIS_NETWORK_OUTPUT


def create_pyvis_network():
    """
    Creates a network visualization using PyVis by loading each processed GeoJSON file from the new folder structure:
    PROCESSED_DIR/{layer_key}.geojson.

    Each node represents a feature (with its unique 'id') colored by its layer.
    An edge is created from a feature to every feature listed in its 'connections' property.

    Additionally, the function logs processing details such as the time taken and network statistics.
    """
    time_start = time.time()

    net = Network(
        height="750px",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        notebook=False,
    )
    nodes = {}

    if not os.path.exists(PROCESSED_DIR):
        print(f"Processed directory not found: {PROCESSED_DIR}")
        return

    for filename in os.listdir(PROCESSED_DIR):
        if not filename.endswith(".geojson"):
            continue

        layer_key = filename.split(".")[0]
        config = csv_files.get(layer_key)
        if config is None:
            print(
                f"Warning: No configuration found for layer key '{layer_key}'. Skipping file {filename}."
            )
            continue

        file_path = os.path.join(PROCESSED_DIR, filename)
        with open(file_path, "r") as f:
            geojson_data = json.load(f)

        for feature in geojson_data.get("features", []):
            feat_id = feature.get("properties", {}).get("id")
            if not feat_id:
                continue
            nodes[feat_id] = {
                "layer": layer_key,
                "label": feat_id,
                "title": config["layer_name"],
                "color": config["color"],
            }

    for node_id, node_data in nodes.items():
        net.add_node(
            node_id,
            label=node_data["label"],
            title=node_data["title"],
            color=node_data["color"],
        )

    for filename in os.listdir(PROCESSED_DIR):
        if not filename.endswith(".geojson"):
            continue

        file_path = os.path.join(PROCESSED_DIR, filename)
        with open(file_path, "r") as f:
            geojson_data = json.load(f)

        for feature in geojson_data.get("features", []):
            source_id = feature.get("properties", {}).get("id")
            if not source_id:
                continue
            connections = feature.get("properties", {}).get("connections", [])
            for target_id in connections:
                if target_id in nodes:
                    net.add_edge(source_id, target_id)

    net.show(PYVIS_NETWORK_OUTPUT, notebook=False)

    time_end = time.time()
    print("PyVis network saved as:", PYVIS_NETWORK_OUTPUT)
    print("Time taken:", time_end - time_start, "seconds")
    print("Network stats: {} nodes, {} edges".format(len(net.nodes), len(net.edges)))
