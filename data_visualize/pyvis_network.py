# pyvis_network.py

import os
import json
from pyvis.network import Network

from config import csv_files, PROCESSED_DIR, PYVIS_NETWORK_OUTPUT


def create_pyvis_network():
    """
    Creates a network visualization using PyVis.
    Each node represents a feature (with its unique 'id') colored by its layer.
    An edge is created from a feature to every feature listed in its 'connections' property.
    """
    net = Network(
        height="750px",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        notebook=False,
    )
    nodes = {}

    for layer_key, config in csv_files.items():
        folder_path = os.path.join(PROCESSED_DIR, layer_key)
        if not os.path.exists(folder_path):
            print(f"Processed folder for layer {layer_key} not found: {folder_path}")
            continue

        for filename in os.listdir(folder_path):
            if filename.endswith(".geojson"):
                file_path = os.path.join(folder_path, filename)
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

    for layer_key, config in csv_files.items():
        folder_path = os.path.join(PROCESSED_DIR, layer_key)
        if not os.path.exists(folder_path):
            continue

        for filename in os.listdir(folder_path):
            if filename.endswith(".geojson"):
                file_path = os.path.join(folder_path, filename)
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
    print("PyVis network saved as:", PYVIS_NETWORK_OUTPUT)
