"""
Configuration constants for connection algorithms.
"""

CONNECTION_SETTINGS = {
    "default_distance_multiplier": 1.0,
    "adaptive_radius": True,
    "min_connections": 1,
    "max_connections_per_entity": 10,
    "prefer_closest": True,
    "validate_topology": True,
    "ignore_isolated_entities": False,
    "connection_score_weights": {
        "distance": 0.7,
        "direction": 0.2,
        "type_compatibility": 0.1,
    },
}
