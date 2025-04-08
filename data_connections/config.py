import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_DIR = "./data"
PROCESSED_DIR = "../output"

CONNECTION_RADIUS = {"far": 7, "mid": 3, "close": 1}

csv_files = {
    "postes_source": {
        "file_path": "postes_source.geojson",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": CONNECTION_RADIUS["far"],
    },
    "postes_electrique": {
        "file_path": "postes_electrique.geojson",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": CONNECTION_RADIUS["mid"],
    },
    "position_geographique": {
        "file_path": "position_geographique.geojson",
        "exclude_connections": [
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        "radius": CONNECTION_RADIUS["mid"],
    },
    "reseau_souterrain_bt": {
        "file_path": "reseau_souterrain_bt.geojson",
        "priority_connections": {
            "postes_source": {
                "priority": 1,
                "radius": CONNECTION_RADIUS["far"],
            },
            "postes_electrique": {
                "priority": 2,
                "radius": CONNECTION_RADIUS["mid"],
            },
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS["mid"],
            },
        },
        "mono_connection_per_endpoint": True,
        "radius": CONNECTION_RADIUS["close"],
    },
    "reseau_bt": {
        "file_path": "reseau_bt.geojson",
        "priority_connections": {
            "postes_source": {
                "priority": 1,
                "radius": CONNECTION_RADIUS["far"],
            },
            "postes_electrique": {
                "priority": 2,
                "radius": CONNECTION_RADIUS["mid"],
            },
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS["mid"],
            },
        },
        "mono_connection_per_endpoint": True,
        "radius": CONNECTION_RADIUS["close"],
    },
    "reseau_souterrain_hta": {
        "file_path": "reseau_souterrain_hta.geojson",
        "solo_connection_if": {
            "postes_source": {
                "priority": 1,
                "radius": CONNECTION_RADIUS["far"],
            },
            "postes_electrique": {
                "priority": 2,
                "radius": CONNECTION_RADIUS["mid"],
            },
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS["mid"],
            },
        },
        "radius": CONNECTION_RADIUS["close"],
    },
}
