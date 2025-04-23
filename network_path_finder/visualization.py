"""
Visualization utilities for the Network Path Finder.
"""

import logging
from pathlib import Path
import json
from .config import LAYER_COLORS


def save_results(point, closest_info, path, path_features, output_file):
    """
    Save the results to a JSON file.

    Args:
        point (Point): The query point
        closest_info (tuple): Information about the closest BT element
        path (list): Path from BT element to source substation
        path_features (list): Full GeoJSON features for the path
        output_file (Path): Output file path
    """
    layer, feature_id, distance, _ = closest_info

    results = {
        "query_point": {"type": "Point", "coordinates": [point.x, point.y]},
        "closest_bt_element": {
            "layer": layer,
            "id": feature_id,
            "distance_km": distance,
        },
        "path_to_source": (
            [{"layer": layer, "id": id_} for layer, id_ in path] if path else None
        ),
        "path_features": path_features,
    }

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logging.info(f"Results saved to {output_file}")


def generate_folium_map(point, path_features, output_file):
    """
    Generate a Folium map visualization of the point and path.

    Args:
        point (Point): The query point
        path_features (list): GeoJSON features in the path
        output_file (Path): Output file path
    """
    try:
        import folium
    except ImportError:
        logging.warning("Folium not installed. Skipping map visualization.")
        return

    # Create the base map centered on the query point
    m = folium.Map(location=[point.y, point.x], zoom_start=15)

    # Add the query point
    folium.Marker(
        location=[point.y, point.x],
        popup="Query Point",
        icon=folium.Icon(color="blue", icon="info-sign"),
    ).add_to(m)

    # Add path features with different styles based on layer
    for feature in path_features:
        properties = feature["properties"]
        layer = properties.get("source_layer", "unknown")
        feature_id = properties.get("id", "unknown")

        geometry = feature["geometry"]
        geometry_type = geometry["type"]

        # Style based on layer
        color = LAYER_COLORS.get(layer, "gray")

        if geometry_type == "Point":
            coords = geometry["coordinates"]
            folium.CircleMarker(
                location=[coords[1], coords[0]],
                radius=8,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                popup=f"{layer}: {feature_id}",
            ).add_to(m)
        elif geometry_type == "LineString":
            coords = geometry["coordinates"]
            line_coords = [[pt[1], pt[0]] for pt in coords]  # Swap lon/lat for folium
            folium.PolyLine(
                locations=line_coords,
                color=color,
                weight=4,
                opacity=0.7,
                popup=f"{layer}: {feature_id}",
            ).add_to(m)

    # Add a legend
    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey;">
    <h4>Legend</h4>
    """
    for layer, color in LAYER_COLORS.items():
        legend_html += f'<p><span style="background-color:{color}; display:inline-block; width:20px; height:10px;"></span> {layer}</p>'
    legend_html += "</div>"

    m.get_root().html.add_child(folium.Element(legend_html))

    # Save map
    m.save(output_file)
    logging.info(f"Map visualization saved to {output_file}")
