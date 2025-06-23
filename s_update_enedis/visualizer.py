"""Create simple visualizations of the electrical network."""

import logging
from typing import Dict
import folium
import geopandas as gpd

from config import OUTPUT_DIR, LAYERS

logger = logging.getLogger(__name__)


def create_network_map(layers: Dict[str, gpd.GeoDataFrame]) -> str:
    """Create an interactive map of the electrical network."""
    layers_wgs84 = {}
    for key, gdf in layers.items():
        layers_wgs84[key] = gdf.to_crs("EPSG:4326")

    all_bounds = []
    for gdf in layers_wgs84.values():
        if not gdf.empty:
            all_bounds.append(gdf.total_bounds)

    if all_bounds:
        minx = min(b[0] for b in all_bounds)
        miny = min(b[1] for b in all_bounds)
        maxx = max(b[2] for b in all_bounds)
        maxy = max(b[3] for b in all_bounds)
        center = [(miny + maxy) / 2, (minx + maxx) / 2]
    else:
        center = [46.603354, 1.888334]

    m = folium.Map(location=center, zoom_start=10, tiles="OpenStreetMap")

    priority_order = sorted(layers_wgs84.keys(), key=lambda x: LAYERS[x].priority)

    for layer_key in priority_order:
        gdf = layers_wgs84[layer_key]
        config = LAYERS[layer_key]

        fg = folium.FeatureGroup(name=config.name)

        for _, feature in gdf.iterrows():
            geom = feature.geometry

            connections = feature.get("connections", [])
            popup_content = f"""
            <b>{config.name}</b><br>
            ID: {feature['id']}<br>
            Connections: {len(connections)}<br>
            """

            if geom.geom_type == "Point":
                folium.CircleMarker(
                    location=[geom.y, geom.x],
                    radius=5 if layer_key.startswith("postes") else 3,
                    popup=popup_content,
                    color=config.color,
                    fill=True,
                    fillColor=config.color,
                    fillOpacity=0.7,
                    weight=2,
                ).add_to(fg)

            elif geom.geom_type == "LineString":
                coords = [[y, x] for x, y in geom.coords]
                folium.PolyLine(
                    locations=coords,
                    popup=popup_content,
                    color=config.color,
                    weight=3,
                    opacity=0.8,
                ).add_to(fg)

        fg.add_to(m)

    folium.LayerControl().add_to(m)

    map_path = OUTPUT_DIR / "network_map.html"
    m.save(str(map_path))
    logger.info(f"Saved map to {map_path}")

    return str(map_path)


def create_connection_graph(layers: Dict[str, gpd.GeoDataFrame]) -> str:
    """Create a simple network graph visualization."""
    try:
        from pyvis.network import Network

        net = Network(
            height="800px",
            width="100%",
            bgcolor="#FFFFFF",
            font_color="black",
            directed=False,
        )

        for layer_key, gdf in layers.items():
            config = LAYERS[layer_key]

            for _, feature in gdf.iterrows():
                net.add_node(
                    feature["id"],
                    label=feature["id"],
                    color=config.color,
                    size=10 if layer_key.startswith("postes") else 5,
                    title=f"{config.name}\nConnections: {len(feature['connections'])}",
                )

        edges_added = set()
        for gdf in layers.values():
            for _, feature in gdf.iterrows():
                source_id = feature["id"]
                for target_id in feature.get("connections", []):
                    edge_key = tuple(sorted([source_id, target_id]))
                    if edge_key not in edges_added:
                        try:
                            net.add_edge(source_id, target_id)
                            edges_added.add(edge_key)
                        except:
                            pass

        graph_path = OUTPUT_DIR / "network_graph.html"
        net.show(str(graph_path), notebook=False)
        logger.info(f"Saved network graph to {graph_path}")

        return str(graph_path)

    except ImportError:
        logger.warning("PyVis not installed, skipping network graph")
        return ""
