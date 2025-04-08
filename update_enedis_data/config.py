# config.py
"""
Configuration du projet Enedis.
Ce fichier définit les constantes et paramètres utilisés pour le chargement, le traitement et la visualisation des données.
"""

import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DIR = os.path.join(BASE_DIR, "output")

CONNECTION_RADIUS = {"far": 7, "mid": 3, "close": 1}

TARGET_CRS = "EPSG:2154"

LAYERS_CONFIG = {
    "postes_source": {
        "csv_file": "poste-source.csv",
        "geojson_file": "postes_source.geojson",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": CONNECTION_RADIUS["far"],
        "color": "red",
        "layer_name": "Postes Source",
    },
    "postes_electrique": {
        "csv_file": "poste-electrique.csv",
        "geojson_file": "postes_electrique.geojson",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": CONNECTION_RADIUS["mid"],
        "color": "green",
        "layer_name": "Postes Electrique",
    },
    "position_geographique": {
        "csv_file": "position-geographique-des-poteaux-hta-et-bt.csv",
        "geojson_file": "position_geographique.geojson",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": CONNECTION_RADIUS["mid"],
        "color": "orange",
        "layer_name": "Poteaux",
    },
    "reseau_souterrain_bt": {
        "csv_file": "reseau-souterrain-bt.csv",
        "geojson_file": "reseau_souterrain_bt.geojson",
        "priority_connections": {
            "postes_source": {"priority": 1, "radius": CONNECTION_RADIUS["far"]},
            "postes_electrique": {"priority": 2, "radius": CONNECTION_RADIUS["mid"]},
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS["mid"],
            },
        },
        "mono_connection_per_endpoint": True,
        "radius": CONNECTION_RADIUS["close"],
        "color": "blue",
        "layer_name": "Reseau Souterrain BT",
    },
    "reseau_bt": {
        "csv_file": "reseau-bt.csv",
        "geojson_file": "reseau_bt.geojson",
        "priority_connections": {
            "postes_source": {"priority": 1, "radius": CONNECTION_RADIUS["far"]},
            "postes_electrique": {"priority": 2, "radius": CONNECTION_RADIUS["mid"]},
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS["mid"],
            },
        },
        "mono_connection_per_endpoint": True,
        "radius": CONNECTION_RADIUS["close"],
        "color": "cadetblue",
        "layer_name": "Reseau Aérienne BT",
    },
    "reseau_souterrain_hta": {
        "csv_file": "reseau-souterrain-hta.csv",
        "geojson_file": "reseau_souterrain_hta.geojson",
        "solo_connection_if": {
            "postes_source": {"priority": 1, "radius": CONNECTION_RADIUS["far"]},
            "postes_electrique": {"priority": 2, "radius": CONNECTION_RADIUS["mid"]},
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS["mid"],
            },
        },
        "radius": CONNECTION_RADIUS["close"],
        "color": "purple",
        "layer_name": "Reseau Souterrain HTA",
    },
}

FOLIUM_MAP_OUTPUT = os.path.join(PROCESSED_DIR, "folium_map.html")
PYVIS_NETWORK_OUTPUT = os.path.join(PROCESSED_DIR, "pyvis_network.html")
