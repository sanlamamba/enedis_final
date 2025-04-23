"""
Visualization module for the Enedis project.

This module generates two interactive visualizations based on GeoJSON files:
    - A Folium map with detailed popups and a legend
    - An interactive network with PyVis

Each function returns or saves its final visualization.
"""

import os
import json
import logging
from typing import Dict, Optional, List, Any
from pathlib import Path

import folium
from folium.plugins import MarkerCluster
import geopandas as gpd

from config import PROCESSED_DIR, FOLIUM_MAP_OUTPUT, PYVIS_NETWORK_OUTPUT, LAYERS_CONFIG
from utils import timed, create_temp_file


def add_legend(map_obj: folium.Map) -> folium.Map:
    """
    Add a legend to the Folium map.

    Args:
        map_obj: Map to add legend to

    Returns:
        Updated map with legend
    """
    # Sort layers by name for consistent legend order
    sorted_layers = sorted(LAYERS_CONFIG.items(), key=lambda x: x[1].layer_name)

    # Create HTML for the legend
    legend_html = (
        """
        <div style="
            position: fixed;
            bottom: 50px; left: 50px;
            width: 220px;
            background-color: rgba(255, 255, 255, 0.8);
            border: 2px solid grey;
            z-index:9999;
            font-size: 14px;
            line-height: 18px;
            padding: 10px;
            box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
        ">
            <div style="text-align: center; font-weight: bold; margin-bottom: 8px;">Légende</div>
        """
        + "".join(
            [
                f"""
            <div style="margin-bottom: 4px;">
                <span style="
                    background: {cfg.color};
                    display: inline-block;
                    width: 12px;
                    height: 12px;
                    margin-right: 6px;
                    border: 1px solid #555;
                "></span>
                {cfg.layer_name}
            </div>
            """
                for key, cfg in sorted_layers
            ]
        )
        + "</div>"
    )

    # Add legend HTML to map
    map_obj.get_root().html.add_child(folium.Element(legend_html))
    return map_obj


def create_popup_content(props: Dict[str, Any], cfg: Any) -> str:
    """
    Build rich HTML content for a popup.

    Args:
        props: Feature properties
        cfg: Layer configuration (name, color, etc.)

    Returns:
        HTML content for the popup
    """
    # Extract properties with defaults
    feature_id = props.get("id", "N/A")
    layer_name = cfg.layer_name
    nom_commune = props.get("nom_commune", "N/A")
    code_commune = props.get("code_commune", "N/A")
    nom_epci = props.get("nom_epci", "N/A")
    code_epci = props.get("code_epci", "N/A")
    nom_departement = props.get("nom_departement", "N/A")
    code_departement = props.get("code_departement", "N/A")
    nom_region = props.get("nom_region", "N/A")
    code_region = props.get("code_region", "N/A")
    connections = props.get("connections", [])
    connection_count = len(connections)

    # Create connection table if connections exist
    if connection_count > 0:
        # Limit to maximum 50 connections in the popup to avoid overloading browser
        display_connections = connections[:50]
        show_more_message = ""
        if len(connections) > 50:
            show_more_message = (
                f"<p><em>Showing 50 of {len(connections)} connections</em></p>"
            )

        connection_rows = "".join(
            f"""
            <tr>
                <td style="padding:4px; border: 1px solid #ddd;">{conn}</td>
            </tr>
            """
            for conn in display_connections
        )
        connections_table = f"""
        <div style="max-height:150px; overflow-y:auto; margin-top:5px;">
            {show_more_message}
            <table style="width:100%; border-collapse: collapse; font-size:12px;">
                <thead>
                    <tr style="background-color: #f0f0f0;">
                        <th style="padding:4px; border: 1px solid #ddd;">ID Connexion</th>
                    </tr>
                </thead>
                <tbody>
                    {connection_rows}
                </tbody>
            </table>
        </div>
        """
    else:
        connections_table = "<p style='margin:0;'>Aucune connexion</p>"

    # Create the complete popup content
    popup_html = f"""
    <div style="min-width:220px; font-size:13px;">
        <h4 style="margin:0 0 5px 0;">{layer_name}</h4>
        <p style="margin:2px 0;"><b>ID:</b> {feature_id}</p>
        <p style="margin:2px 0;"><b>Commune:</b> {nom_commune} ({code_commune})</p>
        <p style="margin:2px 0;"><b>EPCI:</b> {nom_epci} ({code_epci})</p>
        <p style="margin:2px 0;"><b>Département:</b> {nom_departement} ({code_departement})</p>
        <p style="margin:2px 0;"><b>Région:</b> {nom_region} ({code_region})</p>
        <p style="margin:2px 0;"><b>Total connexions:</b> {connection_count}</p>
        <p style="margin:2px 0;"><b>Détails des connexions:</b></p>
        {connections_table}
    </div>
    """
    return popup_html


@timed
def create_folium_map(
    layers_input: Optional[Dict[str, gpd.GeoDataFrame]] = None,
) -> str:
    """
    Create and save an interactive Folium map based on GeoJSON files in PROCESSED_DIR.

    Args:
        layers_input: Optional dictionary of GeoDataFrames to use instead of reading files

    Returns:
        Path to the saved HTML file
    """
    # Create map centered on France
    m = folium.Map(
        location=[46.603354, 1.888334],
        zoom_start=6,
        tiles="OpenStreetMap",
        control_scale=True,
    )

    # Add TileLayer control
    folium.TileLayer("CartoDB positron", name="Light Map").add_to(m)
    folium.TileLayer("CartoDB dark_matter", name="Dark Map").add_to(m)

    # Check if processed directory exists
    if not os.path.exists(PROCESSED_DIR):
        logging.error(f"Processed data directory not found: {PROCESSED_DIR}")
        return ""

    # If layers are provided, use them directly rather than reading files
    if layers_input:
        for layer_key, gdf in layers_input.items():
            cfg = LAYERS_CONFIG.get(layer_key)
            if cfg is None:
                logging.warning(
                    f"No configuration found for layer {layer_key}, skipping."
                )
                continue

            _add_layer_to_folium(m, layer_key, cfg, gdf=gdf)
    else:
        # Otherwise process GeoJSON files
        for filename in os.listdir(PROCESSED_DIR):
            if not filename.endswith(".geojson"):
                continue

            layer_key = filename.split(".")[0]
            cfg = LAYERS_CONFIG.get(layer_key)
            if cfg is None:
                logging.warning(
                    f"No configuration found for layer {layer_key}, skipping."
                )
                continue

            _add_layer_to_folium(m, layer_key, cfg)

    # Add layer control
    folium.LayerControl(collapsed=False).add_to(m)

    # Add legend
    add_legend(m)

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(FOLIUM_MAP_OUTPUT), exist_ok=True)

    # Save the map
    m.save(FOLIUM_MAP_OUTPUT)
    logging.info(f"Folium map saved to: {FOLIUM_MAP_OUTPUT}")

    return str(FOLIUM_MAP_OUTPUT)


def _add_layer_to_folium(
    m: folium.Map, layer_key: str, cfg: Any, gdf: Optional[gpd.GeoDataFrame] = None
):
    """
    Add a specific layer to a Folium map.

    Args:
        m: Folium map to add layer to
        layer_key: Layer key
        cfg: Layer configuration
        gdf: Optional GeoDataFrame to use instead of reading file
    """
    # Create a feature group for this layer
    fg = folium.FeatureGroup(name=cfg.layer_name, show=(layer_key == "postes_source"))
    marker_cluster = MarkerCluster(name=f"{cfg.layer_name} Points").add_to(fg)

    if gdf is not None:
        # Use provided GeoDataFrame
        _process_layer_features(gdf, fg, marker_cluster, cfg)
    else:
        # Read GeoJSON file
        file_path = os.path.join(PROCESSED_DIR, cfg.geojson_file)

        try:
            with open(file_path, "r") as f:
                geojson_data = json.load(f)

            for feature in geojson_data.get("features", []):
                geom = feature.get("geometry", {})
                props = feature.get("properties", {})
                popup_content = create_popup_content(props, cfg)

                # Add geometry to map
                if geom.get("type") == "Point":
                    coords = geom.get("coordinates")
                    if coords and isinstance(coords, list) and len(coords) >= 2:
                        folium.Marker(
                            location=[coords[1], coords[0]],
                            popup=folium.Popup(popup_content, max_width=300),
                            tooltip=cfg.layer_name,
                            icon=folium.Icon(color=cfg.color),
                        ).add_to(marker_cluster)
                elif geom.get("type") == "LineString":
                    coords = geom.get("coordinates")
                    if coords:
                        line_coords = [[pt[1], pt[0]] for pt in coords]
                        folium.PolyLine(
                            locations=line_coords,
                            popup=folium.Popup(popup_content, max_width=300),
                            tooltip=cfg.layer_name,
                            color=cfg.color,
                            weight=3,
                        ).add_to(fg)
        except Exception as e:
            logging.error(f"Error processing layer {layer_key}: {e}")

    # Add feature group to map
    m.add_child(fg)


def _process_layer_features(
    gdf: gpd.GeoDataFrame,
    fg: folium.FeatureGroup,
    marker_cluster: MarkerCluster,
    cfg: Any,
):
    """
    Process features from a GeoDataFrame for adding to a Folium map.

    Args:
        gdf: GeoDataFrame containing features
        fg: Feature group to add to
        marker_cluster: Marker cluster for points
        cfg: Layer configuration
    """
    for _, row in gdf.iterrows():
        geom = row.geometry
        props = {k: v for k, v in row.items() if k != "geometry"}
        popup_content = create_popup_content(props, cfg)

        # Add geometry to map based on type
        if geom.geom_type == "Point":
            folium.Marker(
                location=[geom.y, geom.x],
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=cfg.layer_name,
                icon=folium.Icon(color=cfg.color),
            ).add_to(marker_cluster)
        elif geom.geom_type == "LineString":
            line_coords = [[y, x] for x, y in geom.coords]
            folium.PolyLine(
                locations=line_coords,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=cfg.layer_name,
                color=cfg.color,
                weight=3,
            ).add_to(fg)


@timed
def create_pyvis_network(
    layers_input: Optional[Dict[str, gpd.GeoDataFrame]] = None,
) -> str:
    """
    Create and save an interactive PyVis network based on GeoJSON files in PROCESSED_DIR.

    Args:
        layers_input: Optional dictionary of GeoDataFrames to use instead of reading files

    Returns:
        Path to the saved HTML file
    """
    from pyvis.network import Network

    # Initialize network with better display parameters
    net = Network(
        height="800px",
        width="100%",
        bgcolor="#FFFFFF",
        font_color="black",
        directed=False,
        select_menu=True,
        filter_menu=True,
    )

    # Add physics options for better visualization
    net.set_options(
        """
    {
      "physics": {
        "barnesHut": {
          "gravitationalConstant": -2000,
          "centralGravity": 0.3,
          "springLength": 95,
          "springConstant": 0.04,
          "damping": 0.09
        },
        "maxVelocity": 50,
        "minVelocity": 0.75,
        "solver": "barnesHut",
        "stabilization": {
          "enabled": true,
          "iterations": 1000
        }
      },
      "interaction": {
        "hover": true,
        "multiselect": true,
        "navigationButtons": true
      }
    }
    """
    )

    nodes = {}

    # Check if processed directory exists
    if not os.path.exists(PROCESSED_DIR):
        logging.error(f"Processed data directory not found: {PROCESSED_DIR}")
        return ""

    # Process layers to collect all nodes first
    if layers_input:
        # Use provided GeoDataFrames
        for layer_key, gdf in layers_input.items():
            cfg = LAYERS_CONFIG.get(layer_key)
            if cfg is None:
                continue

            # Add nodes from this layer
            for _, feature in gdf.iterrows():
                feat_id = feature.get("id")
                if not feat_id:
                    continue

                # Store node data
                nodes[feat_id] = {
                    "layer": layer_key,
                    "label": feat_id,
                    "title": cfg.layer_name,
                    "color": cfg.color,
                    "value": len(feature.get("connections", []))
                    + 1,  # Size based on connection count
                }
    else:
        # Read from GeoJSON files
        for filename in os.listdir(PROCESSED_DIR):
            if not filename.endswith(".geojson"):
                continue

            layer_key = filename.split(".")[0]
            cfg = LAYERS_CONFIG.get(layer_key)
            if cfg is None:
                continue

            file_path = os.path.join(PROCESSED_DIR, filename)
            with open(file_path, "r") as f:
                geojson_data = json.load(f)

            for feature in geojson_data.get("features", []):
                feat_id = feature.get("properties", {}).get("id")
                if not feat_id:
                    continue

                # Get connection count for sizing nodes
                connections = feature.get("properties", {}).get("connections", [])

                # Store node data
                nodes[feat_id] = {
                    "layer": layer_key,
                    "label": feat_id,
                    "title": cfg.layer_name,
                    "color": cfg.color,
                    "value": len(connections) + 1,  # Size based on connection count
                }

    # Add nodes to network
    for node_id, node_data in nodes.items():
        net.add_node(
            node_id,
            label=node_data["label"],
            title=f"{node_data['title']}: {node_id}",
            color=node_data["color"],
            value=node_data["value"],
            group=node_data["layer"],  # Group by layer for better visualization
        )

    # Add edges (connections)
    edges_added = set()  # Track added edges to prevent duplicates

    if layers_input:
        # Use provided GeoDataFrames
        for layer_key, gdf in layers_input.items():
            for _, feature in gdf.iterrows():
                source_id = feature.get("id")
                if not source_id:
                    continue

                connections = feature.get("connections", [])
                for target_id in connections:
                    # Create a unique edge identifier
                    edge_id = tuple(sorted([source_id, target_id]))

                    # Add edge if not already added and target exists
                    if edge_id not in edges_added and target_id in nodes:
                        net.add_edge(source_id, target_id)
                        edges_added.add(edge_id)
    else:
        # Read from GeoJSON files
        for filename in os.listdir(PROCESSED_DIR):
            if not filename.endswith(".geojson"):
                continue

            file_path = os.path.join(PROCESSED_DIR, filename)
            with open(file_path, "r") as f:
                geojson_data = json.load(f)

            for feature in geojson_data.get("features", []):
                source_id = feature.get("properties", {}).get("id")
                if not source_id:
                    continue

                connections = feature.get("properties", {}).get("connections", [])
                for target_id in connections:
                    # Create a unique edge identifier
                    edge_id = tuple(sorted([source_id, target_id]))

                    # Add edge if not already added and target exists
                    if edge_id not in edges_added and target_id in nodes:
                        net.add_edge(source_id, target_id)
                        edges_added.add(edge_id)

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(PYVIS_NETWORK_OUTPUT), exist_ok=True)

    # Save the network visualization
    net.show(PYVIS_NETWORK_OUTPUT, notebook=False)
    logging.info(f"PyVis network saved to: {PYVIS_NETWORK_OUTPUT}")

    return str(PYVIS_NETWORK_OUTPUT)


@timed
def create_heatmap(layers_input: Dict[str, gpd.GeoDataFrame]) -> str:
    """
    Create a heatmap showing connection density.

    Args:
        layers_input: Dictionary of GeoDataFrames

    Returns:
        Path to saved heatmap HTML file
    """
    import numpy as np
    from folium.plugins import HeatMap

    # Initialize map
    m = folium.Map(
        location=[46.603354, 1.888334], zoom_start=6, tiles="CartoDB positron"
    )

    # Prepare data for heatmap
    heatmap_data = []

    for layer_key, gdf in layers_input.items():
        # Skip layers that aren't points (like LineString)
        for _, row in gdf.iterrows():
            if row.geometry and row.geometry.geom_type == "Point":
                # Weight by number of connections
                weight = len(row.get("connections", []))
                if weight > 0:
                    heatmap_data.append(
                        [
                            row.geometry.y,  # latitude
                            row.geometry.x,  # longitude
                            weight,  # intensity
                        ]
                    )

    # If we have data, add the heatmap
    if heatmap_data:
        HeatMap(
            heatmap_data,
            radius=15,
            blur=10,
            gradient={0.4: "blue", 0.6: "lime", 0.8: "yellow", 1.0: "red"},
        ).add_to(m)

    # Save the heatmap
    output_path = os.path.join(PROCESSED_DIR, "connection_heatmap.html")
    m.save(output_path)
    logging.info(f"Connection heatmap saved to: {output_path}")

    return output_path


def generate_all_visualizations(layers: Dict[str, gpd.GeoDataFrame]) -> Dict[str, str]:
    """
    Generate all visualization types from the provided layers.

    Args:
        layers: Dictionary of GeoDataFrames by layer key

    Returns:
        Dictionary mapping visualization type to output file path
    """
    visualization_outputs = {}

    try:
        # Generate Folium map
        folium_output = create_folium_map(layers)
        visualization_outputs["folium_map"] = folium_output

        # Generate PyVis network
        pyvis_output = create_pyvis_network(layers)
        visualization_outputs["pyvis_network"] = pyvis_output

        # Generate heatmap
        heatmap_output = create_heatmap(layers)
        visualization_outputs["heatmap"] = heatmap_output

    except Exception as e:
        logging.error(f"Error generating visualizations: {e}")

    return visualization_outputs
