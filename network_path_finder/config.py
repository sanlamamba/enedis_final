"""
Configuration settings for the Network Path Finder.
"""

import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

TARGET_LONGITUDE = 2.3522
TARGET_LATITUDE = 48.8566

DATA_DIR = Path("./output")

OUTPUT_FILE = Path("./network_path_results.json")
MAP_OUTPUT_FILE = Path("./network_path_map.html")

BT_LAYERS = ["reseau_bt", "reseau_souterrain_bt"]
SOURCE_SUBSTATION_LAYER = "postes_source"
ALL_LAYERS = [
    "postes_source",
    "postes_electrique",
    "position_geographique",
    "reseau_souterrain_bt",
    "reseau_bt",
    "reseau_hta",
    "reseau_souterrain_hta",
]

LAYER_COLORS = {
    "postes_source": "red",
    "postes_electrique": "green",
    "position_geographique": "orange",
    "reseau_souterrain_bt": "blue",
    "reseau_bt": "cadetblue",
    "reseau_hta": "red",
    "reseau_souterrain_hta": "purple",
}
