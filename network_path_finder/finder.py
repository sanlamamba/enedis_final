"""
Enhanced finder module with robust path finding strategies.
"""

import logging
from shapely.geometry import Point

from config import (
    DATA_DIR,
    OUTPUT_FILE,
    MAP_OUTPUT_FILE,
    MAX_BT_DISTANCE,
)
from loader import load_geojson_layers, build_network_lookup
from geo import find_closest_bt_elements, extract_path_features
from pathfinding import find_path_with_fallbacks  # Using the improved pathfinding
from visualization import save_results, generate_folium_map


def find_closest_path(
    lon=None, lat=None, data_dir=None, output_file=None, map_file=None
):
    """
    Enhanced path finding with robust fallback strategies.

    Strategies used:
    1. Direct path finding
    2. Relaxed connection search
    3. Component bridging
    4. Multiple hop bridging
    """
    lon = lon
    lat = lat
    data_dir = data_dir if data_dir is not None else DATA_DIR
    output_file = output_file if output_file is not None else OUTPUT_FILE
    map_file = map_file if map_file is not None else MAP_OUTPUT_FILE

    point = Point(lon, lat)
    logging.info(f"Finding closest BT elements to point: ({lon}, {lat})")

    layers = load_geojson_layers(data_dir)
    if not layers:
        logging.error("No layers loaded. Check data directory path.")
        return None

    # Increase search to get more candidates
    closest_elements = find_closest_bt_elements(point, layers, max_elements=20)
    if not closest_elements:
        logging.error(f"No BT elements found within {MAX_BT_DISTANCE} km of the point.")
        return None

    network_lookup = build_network_lookup(layers)

    # Debug: Log available source substations
    source_count = 0
    for node_id, data in network_lookup.items():
        if data["layer"] == "postes_source":
            source_count += 1
    logging.info(f"Found {source_count} source substations in network")

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

        # Use improved path finding with fallbacks
        path = find_path_with_fallbacks(layer, feature_id, network_lookup, layers)

        if path:
            # Filter out bridge markers for display
            display_path = [p for p in path if p[0] != "bridge"]
            logging.info(
                f"Path found! {len(display_path)} elements in path to source substation."
            )
            for j, (layer_name, id_) in enumerate(display_path):
                logging.info(f"  Path element {j+1}: {layer_name}: {id_}")

            # Extract features for the real network elements
            path_features = extract_path_features(display_path, layers)

            result_info = {
                "closest_info": closest_info,
                "path": display_path,
                "path_features": path_features,
            }
            success = True
            break
        else:
            logging.warning(
                f"No path found from {feature_id} with all strategies. Trying next closest BT element..."
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
