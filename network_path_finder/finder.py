"""
Main module for the Network Path Finder.
"""

import logging
from shapely.geometry import Point

from config import (
    DATA_DIR,
    OUTPUT_FILE,
    MAP_OUTPUT_FILE,
    TARGET_LONGITUDE,
    TARGET_LATITUDE,
    MAX_BT_DISTANCE,
)
from loader import load_geojson_layers, build_network_lookup
from geo import find_closest_bt_elements, extract_path_features
from pathfinding import find_path_to_source
from visualization import save_results, generate_folium_map


def find_closest_path(
    lon=None, lat=None, data_dir=None, output_file=None, map_file=None
):
    """
    Main function to find the closest BT element and trace a path to a source substation.
    If the closest BT element doesn't have a path, try the next closest one.

    Args:
        lon (float, optional): Longitude coordinate
        lat (float, optional): Latitude coordinate
        data_dir (Path, optional): Directory containing GeoJSON layers
        output_file (Path, optional): Path to save results JSON
        map_file (Path, optional): Path to save visualization map

    Returns:
        dict: Dictionary containing results with keys:
            - query_point
            - closest_bt_element
            - path_to_source
            - path_features
    """
    lon = lon if lon is not None else TARGET_LONGITUDE
    lat = lat if lat is not None else TARGET_LATITUDE
    data_dir = data_dir if data_dir is not None else DATA_DIR
    output_file = output_file if output_file is not None else OUTPUT_FILE
    map_file = map_file if map_file is not None else MAP_OUTPUT_FILE

    point = Point(lon, lat)
    logging.info(f"Finding closest BT elements to point: ({lon}, {lat})")

    layers = load_geojson_layers(data_dir)
    if not layers:
        logging.error("No layers loaded. Check data directory path.")
        return None

    closest_elements = find_closest_bt_elements(point, layers)
    if not closest_elements:
        logging.error(f"No BT elements found within {MAX_BT_DISTANCE} km of the point.")
        return None

    network_lookup = build_network_lookup(layers)

    success = False
    result_info = None

    for i, closest_info in enumerate(closest_elements):
        layer, feature_id, distance, feature = closest_info

        # Check if we've exceeded the maximum distance
        if distance > MAX_BT_DISTANCE:
            logging.warning(
                f"Maximum distance of {MAX_BT_DISTANCE} km exceeded. Stopping search."
            )
            break

        logging.info(
            f"Attempt #{i+1}: Finding path from {feature_id} in layer {layer} at {distance:.2f} km"
        )

        path = find_path_to_source(layer, feature_id, network_lookup)

        if path:
            logging.info(
                f"Path found! {len(path)} elements in path to source substation."
            )
            for j, (layer_name, id_) in enumerate(path):
                logging.info(f"  Path element {j+1}: {layer_name}: {id_}")

            path_features = extract_path_features(path, layers)

            result_info = {
                "closest_info": closest_info,
                "path": path,
                "path_features": path_features,
            }
            success = True
            break
        else:
            logging.warning(
                f"No path found from {feature_id}. Trying next closest BT element..."
            )

    results = {
        "query_point": {"type": "Point", "coordinates": [point.x, point.y]},
        "success": success,
    }

    if success and result_info:
        layer, feature_id, distance, _ = result_info["closest_info"]
        path = result_info["path"]
        path_features = result_info["path_features"]

        results.update(
            {
                "closest_bt_element": {
                    "layer": layer,
                    "id": feature_id,
                    "distance_km": distance,
                },
                "path_to_source": [{"layer": l, "id": i} for l, i in path],
                "path_features": path_features,
            }
        )

        save_results(
            point, result_info["closest_info"], path, path_features, output_file
        )

        generate_folium_map(point, path_features, map_file)
    else:
        if closest_elements:
            layer, feature_id, distance, _ = closest_elements[0]
            results["closest_bt_element"] = {
                "layer": layer,
                "id": feature_id,
                "distance_km": distance,
            }
        logging.error(
            "No path to source substation found after trying all nearby BT elements."
        )

    return results
