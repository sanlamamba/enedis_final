from collections import deque
from typing import Tuple, List, Dict, Optional, Set


def collect_elements_to_query(
    start_id: str, connections_cache: Dict, exploration_limit: int = 50
) -> Set[str]:
    elements_to_query = set()
    temp_queue = deque([start_id])
    temp_visited = set()

    while temp_queue and len(temp_visited) < exploration_limit:
        current_id = temp_queue.popleft()
        if current_id in temp_visited:
            continue

        temp_visited.add(current_id)
        elements_to_query.add(current_id)

        if current_id in connections_cache:
            for conn_id in connections_cache[current_id]["connections"]:
                temp_queue.append(conn_id)

    return elements_to_query


def find_path_bfs(
    start_id: str, connections_cache: Dict, target_layer: str, max_depth: int = 10
) -> Tuple[Optional[str], List[str]]:
    queue = deque([(start_id, [start_id])])
    visited = set()

    while queue:
        current_id, path = queue.popleft()

        if len(path) > max_depth:
            continue

        if current_id in visited:
            continue
        visited.add(current_id)

        if current_id not in connections_cache:
            continue

        element_info = connections_cache[current_id]

        if element_info["layer"] == target_layer:
            return current_id, path

        for connected_id in element_info["connections"]:
            if connected_id not in visited:
                queue.append((connected_id, path + [connected_id]))

    return None, []


def parse_connections(connections_str) -> List[str]:
    if not connections_str or connections_str is None:
        return []
    return [conn.strip() for conn in str(connections_str).split(",") if conn.strip()]
