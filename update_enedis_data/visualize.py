# visualize.py
"""
Module de visualisation pour le projet Enedis.

Ce module génère deux visualisations interactives basées sur les fichiers GeoJSON :
    - Une carte Folium avec popups détaillées et une légende.
    - Un réseau interactif avec PyVis.

Chaque fonction renvoie ou sauvegarde sa visualisation finale.
"""

import os
import json
import folium
from folium.plugins import MarkerCluster
from config import PROCESSED_DIR, FOLIUM_MAP_OUTPUT, PYVIS_NETWORK_OUTPUT, LAYERS_CONFIG


def add_legend(map_obj):
    """
    Ajoute une légende à la carte Folium.

    Parameters:
        map_obj (folium.Map): Carte à laquelle ajouter la légende.

    Returns:
        folium.Map: La carte mise à jour avec la légende.
    """
    sorted_layers = sorted(
        LAYERS_CONFIG.items(), key=lambda x: x[1].get("layer_name", "")
    )
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
                background: {cfg.get('color', 'blue')};
                display: inline-block;
                width: 12px;
                height: 12px;
                margin-right: 6px;
                border: 1px solid #555;
            "></span>
            {cfg.get('layer_name','')}
        </div>
        """
                for key, cfg in sorted_layers
            ]
        )
        + "</div>"
    )
    map_obj.get_root().html.add_child(folium.Element(legend_html))
    return map_obj


def create_popup_content(props, cfg):
    """
    Construit le contenu HTML riche pour une popup.

    Parameters:
        props (dict): Propriétés de la feature.
        cfg (dict): Configuration de la couche (nom, couleur, etc.).

    Returns:
        str: Contenu HTML de la popup.
    """
    feature_id = props.get("id", "N/A")
    layer_name = cfg.get("layer_name", "Couche")
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

    if connection_count > 0:
        connection_rows = "".join(
            f"""
            <tr>
                <td style="padding:4px; border: 1px solid #ddd;">{conn}</td>
            </tr>
            """
            for conn in connections
        )
        connections_table = f"""
        <div style="max-height:150px; overflow-y:auto; margin-top:5px;">
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


def create_folium_map():
    """
    Crée et sauvegarde une carte interactive Folium basée sur les fichiers GeoJSON du répertoire PROCESSED_DIR.

    Returns:
        None
    """
    m = folium.Map(location=[46.603354, 1.888334], zoom_start=6)
    if not os.path.exists(PROCESSED_DIR):
        print(f"Répertoire des données traitées introuvable : {PROCESSED_DIR}")
        return

    for filename in os.listdir(PROCESSED_DIR):
        if not filename.endswith(".geojson"):
            continue
        layer_key = filename.split(".")[0]
        cfg = LAYERS_CONFIG.get(layer_key)
        if cfg is None:
            print(f"Aucune configuration trouvée pour la couche {layer_key}, passage.")
            continue

        fg = folium.FeatureGroup(name=cfg.get("layer_name", layer_key), show=False)
        marker_cluster = MarkerCluster(
            name=f"{cfg.get('layer_name', layer_key)} Points"
        ).add_to(fg)
        file_path = os.path.join(PROCESSED_DIR, filename)
        with open(file_path, "r") as f:
            geojson_data = json.load(f)

        for feature in geojson_data.get("features", []):
            geom = feature.get("geometry", {})
            props = feature.get("properties", {})
            popup_content = create_popup_content(props, cfg)
            if geom.get("type") == "Point":
                coords = geom.get("coordinates")
                if coords and isinstance(coords, list) and len(coords) >= 2:
                    folium.Marker(
                        location=[coords[1], coords[0]],
                        popup=folium.Popup(popup_content, max_width=300),
                        tooltip=cfg.get("layer_name", ""),
                        icon=folium.Icon(color=cfg.get("color", "blue")),
                    ).add_to(marker_cluster)
            elif geom.get("type") == "LineString":
                coords = geom.get("coordinates")
                if coords:
                    line_coords = [[pt[1], pt[0]] for pt in coords]
                    folium.PolyLine(
                        locations=line_coords,
                        popup=folium.Popup(popup_content, max_width=300),
                        tooltip=cfg.get("layer_name", ""),
                        color=cfg.get("color", "blue"),
                        weight=3,
                    ).add_to(fg)
        m.add_child(fg)

    folium.LayerControl(collapsed=False).add_to(m)
    add_legend(m)
    m.save(FOLIUM_MAP_OUTPUT)
    print("Carte Folium sauvegardée à :", FOLIUM_MAP_OUTPUT)


def create_pyvis_network():
    """
    Crée et sauvegarde un réseau interactif PyVis basé sur les fichiers GeoJSON du répertoire PROCESSED_DIR.

    Returns:
        None
    """
    from pyvis.network import Network

    net = Network(
        height="1024px",
        width="100%",
        bgcolor="#FFFFFF",
        font_color="black",
        directed=False,
    )
    nodes = {}

    if not os.path.exists(PROCESSED_DIR):
        print(f"Répertoire des données traitées introuvable : {PROCESSED_DIR}")
        return

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
            nodes[feat_id] = {
                "layer": layer_key,
                "label": feat_id,
                "title": cfg.get("layer_name", layer_key),
                "color": cfg.get("color", "blue"),
            }
    for node_id, node_data in nodes.items():
        net.add_node(
            node_id,
            label=node_data["label"],
            title=node_data["title"],
            color=node_data["color"],
        )
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
                if target_id in nodes:
                    net.add_edge(source_id, target_id)
    net.show(PYVIS_NETWORK_OUTPUT, notebook=False)
    print("Réseau PyVis sauvegardé à :", PYVIS_NETWORK_OUTPUT)
