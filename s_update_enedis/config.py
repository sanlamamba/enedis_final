"""Configuration for Enedis electrical grid processing."""

from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "faviere"
OUTPUT_DIR = BASE_DIR / "output"

OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

CONN_RADIUS_HIGH = 50.0  # meters
CONN_RADIUS_MEDIUM = 30.0  # meters
CONN_RADIUS_LOW = 10.0  # meters


@dataclass
class LayerConfig:
    name: str
    csv_file: str
    color: str
    connection_radius: float  # meters
    priority: int  # 1 = highest priority (source), higher = lower priority
    can_connect_to: List[str]


LAYERS = {
    "postes_source": LayerConfig(
        name="Source Substations",
        csv_file="poste-source.csv",
        color="red",
        connection_radius=CONN_RADIUS_HIGH,
        priority=1,
        can_connect_to=["reseau_hta", "reseau_souterrain_hta"],
    ),
    "postes_electrique": LayerConfig(
        name="Electrical Substations",
        csv_file="poste-electrique.csv",
        color="green",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=2,
        can_connect_to=[
            "reseau_hta",
            "reseau_souterrain_hta",
            "reseau_bt",
            "reseau_souterrain_bt",
        ],
    ),
    "position_geographique": LayerConfig(
        name="Electrical Poles",
        csv_file="position-geographique-des-poteaux-hta-et-bt.csv",
        color="orange",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=3,
        can_connect_to=["reseau_bt", "reseau_hta"],
    ),
    "reseau_hta": LayerConfig(
        name="HTA Network",
        csv_file="reseau-hta.csv",
        color="darkred",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=4,
        can_connect_to=["postes_source", "postes_electrique", "reseau_hta"],
    ),
    "reseau_souterrain_hta": LayerConfig(
        name="Underground HTA Network",
        csv_file="reseau-souterrain-hta.csv",
        color="purple",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=4,
        can_connect_to=["postes_source", "postes_electrique", "reseau_souterrain_hta"],
    ),
    "reseau_bt": LayerConfig(
        name="BT Network",
        csv_file="reseau-bt.csv",
        color="blue",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=5,
        can_connect_to=["postes_electrique", "position_geographique", "reseau_bt"],
    ),
    "reseau_souterrain_bt": LayerConfig(
        name="Underground BT Network",
        csv_file="reseau-souterrain-bt.csv",
        color="darkblue",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=5,
        can_connect_to=[
            "postes_electrique",
            "position_geographique",
            "reseau_souterrain_bt",
        ],
    ),
}

TARGET_CRS = "EPSG:2154"
