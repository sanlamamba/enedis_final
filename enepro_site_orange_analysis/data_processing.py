import json
import pandas as pd
from typing import Dict, List, Any
from pathfinding import parse_connections


def process_connections_batch(query_result: pd.DataFrame) -> Dict[str, Dict]:
    connections_dict = {}

    for _, row in query_result.iterrows():
        element_id = row["id"]
        connections = parse_connections(row["connections"])

        connections_dict[element_id] = {
            "connections": connections,
            "layer": row["layer"],
        }

    return connections_dict


def create_site_result(
    site_row: pd.Series,
    actual_poste_source: str = None,
    path_elements: List[str] = None,
) -> Dict[str, Any]:
    path_length = len(path_elements) if path_elements else 0

    return {
        "id": site_row["id"],
        "city": site_row["city"],
        "gpsx": site_row["gpsx"],
        "gpsy": site_row["gpsy"],
        "poste_source_proche": site_row.get("closest_poste_source_id"),
        "poste_source_proche_distance": site_row.get("poste_distance"),
        "entry_point_id": site_row.get("closest_bt_id"),
        "entry_point_distance": site_row.get("bt_distance"),
        "actual_poste_source": actual_poste_source,
        "path_elements": json.dumps(path_elements) if path_elements else None,
        "path_length": path_length,
    }


def process_batch_dataframe(batch_df: pd.DataFrame, process_func) -> pd.DataFrame:
    if batch_df.empty:
        return pd.DataFrame()

    results = [process_func(row) for _, row in batch_df.iterrows()]
    return pd.DataFrame(results)
