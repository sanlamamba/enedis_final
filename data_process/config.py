import os

# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_DIR = "../data3"
PROCESSED_DIR = "../output"
os.makedirs(PROCESSED_DIR, exist_ok=True)

TARGET_CRS = "EPSG:2154"
# TODO Refactor #1: Use a better configuration structure
csv_files = {
    "postes_source": {
        "file_path": "poste-source.csv",
        "color": "red",
        "layer_name": "Postes Source",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "priority_connections": None,
        "max_connections": None,
        "connection_radius": 10,
        "order": 1,
    },
    "postes_electrique": {
        "file_path": "poste-electrique.csv",
        "color": "green",
        "layer_name": "Postes Electrique",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "priority_connections": None,
        "max_connections": None,
        "connection_radius": 10,
        "order": 2,
    },
    "position_geographique": {
        "file_path": "position-geographique-des-poteaux-hta-et-bt.csv",
        "color": "green",
        "layer_name": "Poteaux",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "priority_connections": None,
        "max_connections": None,
        "connection_radius": 10,
        "order": 3,
    },
    "reseau_souterrain_bt": {
        "file_path": "reseau-souterrain-bt.csv",
        "color": "blue",
        "layer_name": "Reseau Souterrain BT",
        "priority_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "exclude_connections": None,
        "max_connections": 1,
        "connection_radius": 5,
        "order": 4,
    },
    "reseau_bt": {
        "file_path": "reseau-bt.csv",
        "color": "cadetblue",
        "layer_name": "Reseau Aerienne BT",
        "priority_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "exclude_connections": None,
        "max_connections": 1,
        "connection_radius": 5,
        "order": 5,
    },
    "reseau_souterrain_hta": {
        "file_path": "reseau-souterrain-hta.csv",
        "color": "purple",
        "layer_name": "Reseau Souterrain HTA",
        "priority_connections": None,
        "exclude_connections": None,
        "max_connections": None,
        "connection_radius": 5,
        "order": 7,
    },
}
