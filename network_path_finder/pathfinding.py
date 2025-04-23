"""
Pathfinding algorithms for the Network Path Finder.
"""

import logging
from collections import deque
from .config import SOURCE_SUBSTATION_LAYER


def find_path_to_source(start_layer, start_id, network_lookup):
    """
    Find a path from the starting BT element to the closest source substation.
    Uses breadth-first search to find the shortest path through connections.

    Args:
        start_layer (str): Layer name of the starting element
        start_id (str): ID of the starting element
        network_lookup (dict): Lookup dictionary of all features by ID

    Returns:
        list: List of (layer, id) tuples representing the path, or None if no path found
    """
    if start_id not in network_lookup:
        logging.error(f"Starting ID {start_id} not found in network")
        return None

    # Breadth-first search
    queue = deque([(start_id, [start_id])])
    visited = {start_id}

    while queue:
        current_id, path = queue.popleft()
        current = network_lookup[current_id]

        # If we've reached a source substation, return the path
        if current["layer"] == SOURCE_SUBSTATION_LAYER:
            path_with_layers = [
                (network_lookup[node_id]["layer"], node_id) for node_id in path
            ]
            return path_with_layers

        # Add all unvisited connections to the queue
        for conn_id in current["connections"]:
            if conn_id in network_lookup and conn_id not in visited:
                visited.add(conn_id)
                queue.append((conn_id, path + [conn_id]))

    logging.warning("No path to source substation found")
    return None
