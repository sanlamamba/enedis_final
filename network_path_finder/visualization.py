"""
Enhanced visualization utilities for the Network Path Finder with improved directional indicators.
"""

import logging
from pathlib import Path
import json
from config import LAYER_COLORS
import folium
from folium.plugins import AntPath
import math


def create_directional_arrow(start_coord, end_coord):
    """Create an arrow symbol pointing in the direction of travel."""
    # Calculate angle of the line
    dx = end_coord[0] - start_coord[0]
    dy = end_coord[1] - start_coord[1]
    angle = math.degrees(math.atan2(dy, dx))

    # Create arrow HTML
    arrow_html = f"""
    <div style="transform: rotate({angle}deg); display: inline-block;">
      ➜
    </div>
    """
    return angle, arrow_html


def add_arrows_to_line(line_coords, color, layer, feature_id):
    """Add directional arrows along a line to show flow direction."""
    arrows = []

    # Add arrows at intervals along the line
    for i in range(len(line_coords) - 1):
        start = line_coords[i]
        end = line_coords[i + 1]

        # Place arrow at midpoint of segment
        mid_point = [(start[0] + end[0]) / 2, (start[1] + end[1]) / 2]

        angle, arrow_html = create_directional_arrow(start, end)

        icon = folium.DivIcon(
            html=f'<div style="color: {color}; font-size: 16px; font-weight: bold; transform: rotate({angle}deg);">→</div>',
            icon_size=(20, 20),
            icon_anchor=(10, 10),
        )

        arrow_marker = folium.Marker(
            location=mid_point, icon=icon, popup=f"{layer}: {feature_id} (direction)"
        )
        arrows.append(arrow_marker)

    return arrows


def save_results(point, closest_info, path, path_features, output_file):
    """
    Save the results to a JSON file with enhanced formatting.
    """
    layer, feature_id, distance, _ = closest_info

    # Add path summary statistics
    path_summary = {
        "total_elements": len(path),
        "unique_layers": len(set(l for l, _ in path)),
        "layer_breakdown": {},
        "total_length_km": 0.0,
    }

    # Count elements by layer
    for layer_name, _ in path:
        path_summary["layer_breakdown"][layer_name] = (
            path_summary["layer_breakdown"].get(layer_name, 0) + 1
        )

    # Calculate total path length for LineString features
    for feature in path_features:
        if feature["geometry"]["type"] == "LineString":
            coords = feature["geometry"]["coordinates"]
            for i in range(len(coords) - 1):
                start = coords[i]
                end = coords[i + 1]
                # Simple distance calculation (not accurate for long distances)
                dist = math.sqrt((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2)
                path_summary["total_length_km"] += (
                    dist * 111.32
                )  # Rough conversion to km

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
        "path_summary": path_summary,
    }

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logging.info(f"Results saved to {output_file}")


def generate_folium_map(point, path_features, output_file):
    """
    Generate an enhanced Folium map visualization with better directional indicators.
    """
    try:
        import folium
        from folium.plugins import AntPath
    except ImportError:
        logging.warning("Folium not installed. Skipping map visualization.")
        return

    # Create the base map centered on the query point
    m = folium.Map(
        location=[point.y, point.x],
        zoom_start=15,
        tiles="cartodbpositron",
        control_scale=True,
    )

    # Add multiple tile layers
    folium.TileLayer("cartodbdark_matter", name="Dark Mode").add_to(m)
    folium.TileLayer("openstreetmap", name="OpenStreetMap").add_to(m)

    # Create a feature group for the path
    path_fg = folium.FeatureGroup(name="Network Path").add_to(m)

    # Add the query point with a distinctive marker
    folium.Marker(
        location=[point.y, point.x],
        popup=folium.Popup(
            """
            <div style='font-family: Arial, sans-serif; min-width: 150px;'>
                <h4>Query Point</h4>
                <p><b>Coordinates:</b><br>
                   Lat: {:.6f}<br>
                   Lon: {:.6f}</p>
            </div>
        """.format(
                point.y, point.x
            ),
            max_width=300,
        ),
        icon=folium.Icon(color="red", icon="crosshairs", prefix="fa"),
        tooltip="Click for details",
    ).add_to(m)

    # Create numbered path sequence
    path_sequence = []

    # Process path features with enhanced styling
    for idx, feature in enumerate(path_features):
        properties = feature["properties"]
        layer = properties.get("source_layer", "unknown")
        feature_id = properties.get("id", "unknown")

        geometry = feature["geometry"]
        geometry_type = geometry["type"]

        # Get color with fallback
        color = LAYER_COLORS.get(layer, "gray")

        # Create detailed popup
        popup_content = f"""
        <div style='font-family: Arial, sans-serif; min-width: 300px;'>
            <h4>Path Element #{idx + 1}</h4>
            <p><b>Layer:</b> {layer}<br>
               <b>ID:</b> {feature_id}<br>
               <b>Type:</b> {layer.replace('_', ' ').title()}</p>
        """

        if "nom_commune" in properties:
            popup_content += f"<p><b>Location:</b> {properties['nom_commune']}, {properties['nom_departement']}</p>"

        if geometry_type == "LineString":
            coords = geometry["coordinates"]
            line_coords = [[pt[1], pt[0]] for pt in coords]

            # Add length estimation
            total_length = 0
            for i in range(len(coords) - 1):
                start = coords[i]
                end = coords[i + 1]
                length = (
                    math.sqrt((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2)
                    * 111320
                )  # Convert to meters
                total_length += length

            popup_content += f"<p><b>Length:</b> {total_length:.1f} meters</p>"

        popup_content += "</div>"

        if geometry_type == "Point":
            coords = geometry["coordinates"]

            # Add numbered circle marker
            folium.CircleMarker(
                location=[coords[1], coords[0]],
                radius=10,
                color="darkblue",
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                popup=popup_content,
                tooltip=f"{idx + 1}: {layer}",
            ).add_to(path_fg)

            # Add number overlay
            folium.Marker(
                location=[coords[1], coords[0]],
                icon=folium.DivIcon(
                    html=f'<div style="color: white; font-weight: bold; text-shadow: 1px 1px 1px black; font-size: 12px;">{idx + 1}</div>',
                    icon_size=(20, 20),
                    icon_anchor=(10, 10),
                ),
            ).add_to(path_fg)

            path_sequence.append([coords[1], coords[0]])

        elif geometry_type == "LineString":
            coords = geometry["coordinates"]
            line_coords = [[pt[1], pt[0]] for pt in coords]

            # Main line with gradient color
            folium.PolyLine(
                locations=line_coords,
                color=color,
                weight=6,
                opacity=0.8,
                popup=popup_content,
                tooltip=f"{idx + 1}: {layer}",
            ).add_to(path_fg)

            # Add directional arrows
            arrows = add_arrows_to_line(line_coords, color, layer, feature_id)
            for arrow in arrows:
                arrow.add_to(path_fg)

            # Add start and end markers
            start_coords = line_coords[0]
            end_coords = line_coords[-1]

            # Start marker (green)
            folium.CircleMarker(
                location=start_coords,
                radius=6,
                color="green",
                fill=True,
                fill_color="lightgreen",
                fill_opacity=1.0,
                popup=f"Start of {feature_id}",
            ).add_to(path_fg)

            # End marker (red)
            folium.CircleMarker(
                location=end_coords,
                radius=6,
                color="red",
                fill=True,
                fill_color="pink",
                fill_opacity=1.0,
                popup=f"End of {feature_id}",
            ).add_to(path_fg)

            path_sequence.extend(line_coords)

    # Add animated path if we have a sequence
    if len(path_sequence) > 1:
        AntPath(
            path_sequence,
            color="red",
            weight=3,
            opacity=0.8,
            dash_array=[10, 20],
            delay=1000,
        ).add_to(m)

    # Enhanced legend with statistics
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: rgba(255, 255, 255, 0.95); padding: 15px; border: 2px solid grey; border-radius: 5px; box-shadow: 3px 3px 3px rgba(0,0,0,0.3);">
        <h4 style="margin-top: 0;">Network Path Legend</h4>
        <div style="margin-bottom: 10px;">
            <p style="margin: 5px 0;"><strong>Total Elements:</strong> {len(path_features)}</p>
        </div>
        <hr style="margin: 10px 0;">
        <h5>Layer Colors:</h5>
    """

    for layer, color in LAYER_COLORS.items():
        count = sum(
            1 for f in path_features if f["properties"].get("source_layer") == layer
        )
        if count > 0:
            legend_html += f'<p style="margin: 5px 0;"><span style="background-color:{color}; display:inline-block; width:20px; height:10px; margin-right: 5px; border: 1px solid black;"></span> {layer} ({count})</p>'

    legend_html += """
        <hr style="margin: 10px 0;">
        <h5>Symbols:</h5>
        <p style="margin: 5px 0;">🔴 Start of line</p>
        <p style="margin: 5px 0;">🔵 End of line</p>
        <p style="margin: 5px 0;">→ Flow direction</p>
        <p style="margin: 5px 0;">🎯 Query point</p>
        <p style="margin: 5px 0;">--- Animated path</p>
    </div>
    """

    m.get_root().html.add_child(folium.Element(legend_html))

    # Add layer control
    folium.LayerControl().add_to(m)

    # Add measurement plugin
    from folium.plugins import MeasureControl

    MeasureControl(primary_length_unit="meters").add_to(m)

    # Save map
    m.save(output_file)
    logging.info(f"Enhanced map visualization saved to {output_file}")
