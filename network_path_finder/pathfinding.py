"""
Improved pathfinding algorithms with fallback strategies for reliable path finding.
"""

import logging
from collections import deque
from typing import List, Optional, Tuple, Dict, Set
from config import SOURCE_SUBSTATION_LAYER


class NetworkBridger:
    """Smart network bridger that can find connections between disconnected components."""

    def __init__(self, layers: Dict):
        self.layers = layers
        self.component_mapping = {}

    def build_components(self, network_lookup: Dict) -> Dict[int, Set[str]]:
        """Build connected components of the network using BFS."""
        visited = set()
        components = {}
        component_id = 0

        for node_id in network_lookup:
            if node_id not in visited:
                component = self._get_connected_component(node_id, network_lookup)
                components[component_id] = component
                for n in component:
                    self.component_mapping[n] = component_id
                component_id += 1

        return components

    def _get_connected_component(self, start_id: str, network_lookup: Dict) -> Set[str]:
        """Get all nodes in the connected component containing start_id."""
        component = set()
        queue = deque([start_id])
        component.add(start_id)

        while queue:
            current_id = queue.popleft()
            if current_id in network_lookup:
                for conn_id in network_lookup[current_id]["connections"]:
                    if conn_id in network_lookup and conn_id not in component:
                        component.add(conn_id)
                        queue.append(conn_id)

        return component

    def find_closest_elements_between_components(
        self, comp1_ids: Set[str], comp2_ids: Set[str], max_distance: float = 1000.0
    ) -> Optional[Tuple[str, str, float]]:
        """Find the closest pair of elements between two components."""
        import numpy as np

        min_distance = float("inf")
        best_pair = None

        # Get geometries for all elements in both components
        comp1_geoms = {}
        comp2_geoms = {}

        for layer_name, gdf in self.layers.items():
            for _, row in gdf.iterrows():
                element_id = row["id"]
                if element_id in comp1_ids:
                    comp1_geoms[element_id] = row.geometry
                elif element_id in comp2_ids:
                    comp2_geoms[element_id] = row.geometry

        # Find closest pair
        for id1, geom1 in comp1_geoms.items():
            for id2, geom2 in comp2_geoms.items():
                distance = geom1.distance(geom2)
                if distance < min_distance:
                    min_distance = distance
                    best_pair = (id1, id2)

        if min_distance <= max_distance:
            return (*best_pair, min_distance)

        return None


def find_path_with_bridging(
    start_layer: str,
    start_id: str,
    network_lookup: Dict,
    layers: Dict[str, "gpd.GeoDataFrame"],
) -> Optional[List[Tuple[str, str]]]:
    """
    Find a path to source substation with smart bridging of disconnected components.

    This function:
    1. Finds path within connected component
    2. If source not found, identifies and bridges gaps to other components
    3. Continues search in bridged components
    """
    # First try standard pathfinding
    path = find_path_to_source(start_layer, start_id, network_lookup)
    if path:
        return path

    logging.info("No direct path found, attempting bridging strategy...")

    # Build connected components
    bridger = NetworkBridger(layers)
    components = bridger.build_components(network_lookup)

    # Identify component containing start node
    if start_id not in bridger.component_mapping:
        logging.error(f"Start ID {start_id} not found in any component")
        return None

    start_component_id = bridger.component_mapping[start_id]
    start_component = components[start_component_id]

    # Check if source is in another component
    source_components = []
    for comp_id, comp_nodes in components.items():
        for node_id in comp_nodes:
            if (
                node_id in network_lookup
                and network_lookup[node_id]["layer"] == SOURCE_SUBSTATION_LAYER
            ):
                source_components.append(comp_id)
                break

    if not source_components:
        logging.warning("No source substations found in any component")
        return None

    # Try to bridge to components containing sources
    for source_comp_id in source_components:
        if source_comp_id == start_component_id:
            continue  # Already checked this component

        source_component = components[source_comp_id]

        # Find closest pair between components
        bridge_info = bridger.find_closest_elements_between_components(
            start_component, source_component, max_distance=2000.0
        )

        if bridge_info:
            bridge_id1, bridge_id2, distance = bridge_info
            logging.info(
                f"Found bridge: {bridge_id1} to {bridge_id2} ({distance:.2f}m)"
            )

            # Path: start -> bridge1 -> bridge2 -> source
            path_to_bridge1 = find_path_to_node(start_id, bridge_id1, network_lookup)
            path_from_bridge2 = find_path_from_node_to_source(
                bridge_id2, network_lookup
            )

            if path_to_bridge1 and path_from_bridge2:
                # Combine paths with bridge
                full_path = path_to_bridge1[:-1]  # Remove last element
                full_path.append((network_lookup[bridge_id1]["layer"], bridge_id1))
                full_path.append(("bridge", f"{bridge_id1}->{bridge_id2}"))
                full_path.append((network_lookup[bridge_id2]["layer"], bridge_id2))
                full_path.extend(path_from_bridge2[1:])  # Skip first element

                return full_path

    return None


def find_path_to_node(
    start_id: str, target_id: str, network_lookup: Dict
) -> Optional[List[Tuple[str, str]]]:
    """Find path from start to a specific target node."""
    if start_id not in network_lookup:
        return None

    queue = deque([(start_id, [start_id])])
    visited = {start_id}

    while queue:
        current_id, path = queue.popleft()

        if current_id == target_id:
            return [(network_lookup[node_id]["layer"], node_id) for node_id in path]

        if current_id in network_lookup:
            for conn_id in network_lookup[current_id]["connections"]:
                if conn_id in network_lookup and conn_id not in visited:
                    visited.add(conn_id)
                    queue.append((conn_id, path + [conn_id]))

    return None


def find_path_from_node_to_source(
    start_id: str, network_lookup: Dict
) -> Optional[List[Tuple[str, str]]]:
    """Find path from specific node to any source substation."""
    if start_id not in network_lookup:
        return None

    queue = deque([(start_id, [start_id])])
    visited = {start_id}

    while queue:
        current_id, path = queue.popleft()
        current = network_lookup[current_id]

        if current["layer"] == SOURCE_SUBSTATION_LAYER:
            return [(network_lookup[node_id]["layer"], node_id) for node_id in path]

        for conn_id in current["connections"]:
            if conn_id in network_lookup and conn_id not in visited:
                visited.add(conn_id)
                queue.append((conn_id, path + [conn_id]))

    return None


def find_path_with_fallbacks(
    start_layer: str,
    start_id: str,
    network_lookup: Dict,
    layers: Dict[str, "gpd.GeoDataFrame"],
) -> Optional[List[Tuple[str, str]]]:
    """
    Find path with multiple fallback strategies.

    Strategies attempted in order:
    1. Direct BFS path finding
    2. Relaxed connection search (using start/end connections)
    3. Proximity-based bridging
    4. Multiple hop bridging
    """
    # Strategy 1: Direct path
    path = find_path_to_source(start_layer, start_id, network_lookup)
    if path:
        logging.info(f"Found direct path with {len(path)} elements")
        return path

    # Strategy 2: Relaxed search using start/end connections for LineStrings
    path = find_path_with_relaxed_connections(start_layer, start_id, network_lookup)
    if path:
        logging.info(f"Found path using relaxed connections with {len(path)} elements")
        return path

    # Strategy 3: Bridging between components
    path = find_path_with_bridging(start_layer, start_id, network_lookup, layers)
    if path:
        logging.info(f"Found path using component bridging with {len(path)} elements")
        return path

    # Strategy 4: Multiple hop search (if all else fails)
    path = find_path_with_multiple_hops(
        start_layer, start_id, network_lookup, layers, max_hops=3
    )
    if path:
        logging.info(f"Found path using multiple hops with {len(path)} elements")
        return path

    logging.warning("All path-finding strategies failed")
    return None


def find_path_with_relaxed_connections(
    start_layer: str, start_id: str, network_lookup: Dict
) -> Optional[List[Tuple[str, str]]]:
    """Path finding using both main connections and start/end connections for LineStrings."""
    if start_id not in network_lookup:
        return None

    # Build expanded network with start/end connections
    expanded_network = {}
    for node_id, node_data in network_lookup.items():
        all_connections = set(node_data["connections"])

        # Add start/end connections if available
        feature = node_data["feature"]
        if "start_connections" in feature:
            all_connections.update(feature["start_connections"])
        if "end_connections" in feature:
            all_connections.update(feature["end_connections"])

        expanded_network[node_id] = {
            "layer": node_data["layer"],
            "connections": list(all_connections),
        }

    # Use BFS with expanded connections
    queue = deque([(start_id, [start_id])])
    visited = {start_id}

    while queue:
        current_id, path = queue.popleft()

        if current_id not in expanded_network:
            continue

        current = expanded_network[current_id]

        if current["layer"] == SOURCE_SUBSTATION_LAYER:
            return [(expanded_network[node_id]["layer"], node_id) for node_id in path]

        for conn_id in current["connections"]:
            if conn_id in expanded_network and conn_id not in visited:
                visited.add(conn_id)
                queue.append((conn_id, path + [conn_id]))

    return None


def find_path_with_multiple_hops(
    start_layer: str,
    start_id: str,
    network_lookup: Dict,
    layers: Dict[str, "gpd.GeoDataFrame"],
    max_hops: int = 3,
) -> Optional[List[Tuple[str, str]]]:
    """
    Find path with multiple bridging hops if necessary.
    """
    current_id = start_id
    full_path = [(start_layer, start_id)]

    for hop in range(max_hops):
        # Find nearest source substation
        path_to_source = find_path_from_node_to_source(current_id, network_lookup)
        if path_to_source:
            full_path.extend(path_to_source[1:])  # Skip first element
            return full_path

        # Find nearest element in a different component
        bridger = NetworkBridger(layers)
        current_component = bridger._get_connected_component(current_id, network_lookup)

        # Find all elements not in current component
        other_elements = set()
        for node_id in network_lookup:
            if node_id not in current_component:
                other_elements.add(node_id)

        if not other_elements:
            break

        # Find closest element to current position
        bridge_info = bridger.find_closest_elements_between_components(
            {current_id}, other_elements, max_distance=5000.0
        )

        if not bridge_info:
            break

        _, next_id, distance = bridge_info
        logging.info(f"Hop {hop+1}: Bridging to {next_id} ({distance:.2f}m)")

        # Add bridge to path
        full_path.append(("bridge", f"{current_id}->{next_id}"))
        full_path.append((network_lookup[next_id]["layer"], next_id))
        current_id = next_id

    return None


# Original function kept for compatibility
def find_path_to_source(
    start_layer: str, start_id: str, network_lookup: Dict
) -> Optional[List[Tuple[str, str]]]:
    """Original BFS path finding function."""
    if start_id not in network_lookup:
        logging.error(f"Starting ID {start_id} not found in network")
        return None

    queue = deque([(start_id, [start_id])])
    visited = {start_id}

    while queue:
        current_id, path = queue.popleft()
        current = network_lookup[current_id]

        if current["layer"] == SOURCE_SUBSTATION_LAYER:
            path_with_layers = [
                (network_lookup[node_id]["layer"], node_id) for node_id in path
            ]
            return path_with_layers

        for conn_id in current["connections"]:
            if conn_id in network_lookup and conn_id not in visited:
                visited.add(conn_id)
                queue.append((conn_id, path + [conn_id]))

    logging.warning(f"No path to source substation found from {start_id}")
    return None
