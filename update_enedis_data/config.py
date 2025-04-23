"""
Configuration module for the Enedis project.
This file defines constants and parameters used for loading, processing and visualizing data.
"""

import os
import logging
import typing as t
from pathlib import Path
from dataclasses import dataclass
from enum import Enum, auto

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)

BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = BASE_DIR / "data/chatillon"
PROCESSED_DIR = BASE_DIR / "output"

PROCESSED_DIR.mkdir(exist_ok=True, parents=True)


class RadiusType(Enum):
    CLOSE = auto()
    MID = auto()
    FAR = auto()


CONNECTION_RADIUS = {RadiusType.CLOSE: 2, RadiusType.MID: 5, RadiusType.FAR: 10}

TARGET_CRS = "EPSG:2154"


@dataclass
class LayerConfig:
    csv_file: str
    geojson_file: str
    radius: int
    color: str
    layer_name: str
    exclude_connections: t.Optional[t.List[str]] = None
    priority_connections: t.Optional[t.Dict[str, t.Dict[str, t.Any]]] = None
    mono_connection_per_endpoint: bool = False
    solo_connection_if: t.Optional[t.Dict[str, t.Dict[str, t.Any]]] = None


# Layer configurations
LAYERS_CONFIG = {
    "postes_source": LayerConfig(
        csv_file="poste-source.csv",
        geojson_file="postes_source.geojson",
        exclude_connections=[
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        radius=CONNECTION_RADIUS[RadiusType.FAR],
        color="red",
        layer_name="Postes Source",
    ),
    "postes_electrique": LayerConfig(
        csv_file="poste-electrique.csv",
        geojson_file="postes_electrique.geojson",
        exclude_connections=[
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        radius=CONNECTION_RADIUS[RadiusType.MID],
        color="green",
        layer_name="Postes Electrique",
    ),
    "position_geographique": LayerConfig(
        csv_file="position-geographique-des-poteaux-hta-et-bt.csv",
        geojson_file="position_geographique.geojson",
        exclude_connections=[
            "postes_source",
            "postes_electrique",
            "position_geographique",
        ],
        radius=CONNECTION_RADIUS[RadiusType.MID],
        color="orange",
        layer_name="Poteaux",
    ),
    "reseau_souterrain_bt": LayerConfig(
        csv_file="reseau-souterrain-bt.csv",
        geojson_file="reseau_souterrain_bt.geojson",
        priority_connections={
            "postes_source": {
                "priority": 1,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
            "postes_electrique": {
                "priority": 2,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
        },
        mono_connection_per_endpoint=True,
        radius=CONNECTION_RADIUS[RadiusType.CLOSE],
        color="blue",
        layer_name="Reseau Souterrain BT",
    ),
    "reseau_bt": LayerConfig(
        csv_file="reseau-bt.csv",
        geojson_file="reseau_bt.geojson",
        priority_connections={
            "postes_source": {
                "priority": 1,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
            "postes_electrique": {
                "priority": 2,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
        },
        mono_connection_per_endpoint=True,
        radius=CONNECTION_RADIUS[RadiusType.CLOSE],
        color="cadetblue",
        layer_name="Reseau Aérienne BT",
    ),
    # "reseau_hta": LayerConfig(
    #     csv_file="reseau-hta.csv",
    #     geojson_file="reseau_hta.geojson",
    #     solo_connection_if={
    #         "postes_source": {
    #             "priority": 1,
    #             "radius": CONNECTION_RADIUS[RadiusType.FAR],
    #         },
    #         "postes_electrique": {
    #             "priority": 2,
    #             "radius": CONNECTION_RADIUS[RadiusType.MID],
    #         },
    #         "position_geographique": {
    #             "priority": 3,
    #             "radius": CONNECTION_RADIUS[RadiusType.MID],
    #         },
    #     },
    #     radius=CONNECTION_RADIUS[RadiusType.CLOSE],
    #     color="red",
    #     layer_name="Reseau Aérienne HTA",
    # ),
    "reseau_souterrain_hta": LayerConfig(
        csv_file="reseau-souterrain-hta.csv",
        geojson_file="reseau_souterrain_hta.geojson",
        solo_connection_if={
            "postes_source": {
                "priority": 1,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
            "postes_electrique": {
                "priority": 2,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
            "position_geographique": {
                "priority": 3,
                "radius": CONNECTION_RADIUS[RadiusType.FAR],
            },
        },
        radius=CONNECTION_RADIUS[RadiusType.CLOSE],
        color="purple",
        layer_name="Reseau Souterrain HTA",
    ),
}

FOLIUM_MAP_OUTPUT = PROCESSED_DIR / "folium_map.html"
PYVIS_NETWORK_OUTPUT = PROCESSED_DIR / "pyvis_network.html"

USE_CLOUD_STORAGE = os.environ.get("USE_CLOUD_STORAGE", "false").lower() == "true"
CLOUD_BUCKET_PROJECT = os.environ.get(
    "CLOUD_BUCKET_PROJECT", "ofr-2kt-valo-reseau-1-lab-prd"
)
CLOUD_BUCKET_NAME = os.environ.get("CLOUD_BUCKET_NAME", "ofr-2kt-valo-enedis").strip()

CLOUD_DATA_DIR = os.environ.get("CLOUD_DATA_DIR", "downloaded")
CLOUD_PROCESSED_DIR = os.environ.get("CLOUD_PROCESSED_DIR", "processed")
BIGQUERY_TABLE_ID = os.environ.get(
    "BIGQUERY_TABLE_ID", "ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis"
)

SHOULD_EXPORT_GEOJSON = (
    os.environ.get("SHOULD_EXPORT_GEOJSON", "true").lower() == "true"
)
SHOULD_EXPORT_SHAPEFILE = (
    os.environ.get("SHOULD_EXPORT_SHAPEFILE", "false").lower() == "true"
)
