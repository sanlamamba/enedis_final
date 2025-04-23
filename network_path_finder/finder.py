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
)
from loader import load_geojson_layers, build_network_lookup
from geo import find_closest_bt_element, extract_path_features
from pathfinding import find_path_to_source
from visualization import save_results, generate_folium_map


def find_closest_path(
    lon=None, lat=None, data_dir=None, output_file=None, map_file=None
):
    """
    Main function to find the closest BT element and trace a path to a source substation.

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
    logging.info(f"Finding closest BT element to point: ({lon}, {lat})")

    layers = load_geojson_layers(data_dir)
    if not layers:
        logging.error("No layers loaded. Check data directory path.")
        return None

    # Find closest BT element
    closest_info = find_closest_bt_element(point, layers)
    if not closest_info:
        logging.error("No BT elements found in the loaded layers.")
        return None

    layer, feature_id, distance, feature = closest_info
    logging.info(
        f"Found closest BT element: {feature_id} in layer {layer} at {distance:.2f} km"
    )

    # Build network lookup for pathfinding
    network_lookup = build_network_lookup(layers)

    # Find path to source substation
    path = find_path_to_source(layer, feature_id, network_lookup)

    # Prepare results
    results = {
        "query_point": {"type": "Point", "coordinates": [point.x, point.y]},
        "closest_bt_element": {
            "layer": layer,
            "id": feature_id,
            "distance_km": distance,
        },
        "path_to_source": None,
        "path_features": None,
    }

    if path:
        logging.info(f"Found path to source substation with {len(path)} elements")
        for i, (layer, id_) in enumerate(path):
            logging.info(f"  {i+1}. {layer}: {id_}")

        # Extract full feature information
        path_features = extract_path_features(path, layers)
        results["path_to_source"] = [{"layer": l, "id": i} for l, i in path]
        results["path_features"] = path_features

        # Save results
        save_results(point, closest_info, path, path_features, output_file)

        # Generate map visualization
        generate_folium_map(point, path_features, map_file)
    else:
        logging.warning("No path to source substation found")

    return results
