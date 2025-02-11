import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_DIR = "./data"
PROCESSED_DIR = "../output"
CONNECTION_RADIUS_METERS = 10

# TODO refacor use a better configuration structure
csv_files = {
    "postes_source": {
        "file_path": "postes_source.geojson",
        "layer_name": "Postes Source",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 10,
    },
    "postes_electrique": {
        "file_path": "postes_electrique.geojson",
        "layer_name": "Postes Electrique",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 5,
    },
    "position_geographique": {
        "file_path": "position_geographique.geojson",
        "layer_name": "Position Geographique",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 5,
    },
    "reseau_souterrain_bt": {
        "file_path": "reseau_souterrain_bt.geojson",
        "layer_name": "Reseau Souterrain BT",
        "priority_connections": {
            "postes_source": 1,
            "postes_electrique": 2,
            "position_geographique": 3,
        },
        "mono_connection_per_endpoint": True,
        "radius": 5,
    },
    "reseau_bt": {
        "file_path": "reseau_bt.geojson",
        "layer_name": "Reseau BT",
        "priority_connections": {
            "postes_source": 1,
            "postes_electrique": 2,
            "position_geographique": 3,
        },
        "mono_connection_per_endpoint": True,
        "radius": 5,
    },
    "reseau_souterrain_hta": {
        "file_path": "reseau_souterrain_hta.geojson",
        "layer_name": "Reseau Souterrain HTA",
        "solo_connection_if": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": 5,
    },
}
