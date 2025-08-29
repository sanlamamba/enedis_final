"""Simplified configuration for cloud-based electrical grid processing."""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import List

LOCAL_OUTPUT_PATH = os.getenv("LOCAL_OUTPUT_PATH", "../output")
LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "../data")

CLOUD_BUCKET_NAME = os.getenv("CLOUD_BUCKET_NAME", "ofr-2kt-valo-enedis")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ofr-2kt-valo-reseau-1-lab-prd")
CLOUD_DATA_PATH = os.getenv("CLOUD_DATA_PATH", "downloaded/full")

TARGET_CRS = "EPSG:2154"

CONN_RADIUS_HIGH = 50.0  # meters
CONN_RADIUS_MEDIUM = 30.0  # meters
CONN_RADIUS_LOW = 10.0  # meters


@dataclass
class LayerConfig:
    """Configuration for each electrical grid layer."""

    name: str
    csv_file: str
    connection_radius: float
    priority: int
    can_connect_to: List[str]


LAYERS = {
    "postes_source": LayerConfig(
        name="Source Substations",
        csv_file="poste-source.csv",
        connection_radius=CONN_RADIUS_HIGH,
        priority=1,
        can_connect_to=["reseau_hta", "reseau_souterrain_hta"],
    ),
    "postes_electrique": LayerConfig(
        name="Electrical Substations",
        csv_file="poste-electrique.csv",
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
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=3,
        can_connect_to=["reseau_bt", "reseau_hta"],
    ),
    "reseau_hta": LayerConfig(
        name="HTA Network",
        csv_file="reseau-hta.csv",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=4,
        can_connect_to=[
            "postes_source",
            "postes_electrique",
            "reseau_hta",
            "reseau_souterrain_hta",
            "reseau_bt",
            "reseau_souterrain_bt",
        ],
    ),
    "reseau_souterrain_hta": LayerConfig(
        name="Underground HTA Network",
        csv_file="reseau-souterrain-hta.csv",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=4,
        can_connect_to=[
            "postes_source",
            "postes_electrique",
            "reseau_souterrain_hta",
            "reseau_hta",
            "reseau_bt",
            "reseau_souterrain_bt",
        ],
    ),
    "reseau_bt": LayerConfig(
        name="BT Network",
        csv_file="reseau-bt.csv",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=5,
        can_connect_to=[
            "postes_electrique",
            "position_geographique",
            "reseau_bt",
            "reseau_souterrain_bt",
            "reseau_souterrain_hta",
            "reseau_hta",
        ],
    ),
    "reseau_souterrain_bt": LayerConfig(
        name="Underground BT Network",
        csv_file="reseau-souterrain-bt.csv",
        connection_radius=CONN_RADIUS_MEDIUM,
        priority=5,
        can_connect_to=[
            "postes_electrique",
            "position_geographique",
            "reseau_souterrain_bt",
            "reseau_bt",
            "reseau_souterrain_hta",
            "reseau_hta",
        ],
    ),
}
