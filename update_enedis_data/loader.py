# loader.py
"""
Module de chargement des données pour le projet Enedis.

Ce module fournit des fonctions pour charger les données depuis des fichiers CSV ou GeoJSON
Les données CSV sont converties en GeoDataFrame en extrayant la géométrie depuis le champ "geo_shape".
Chaque couche est définie dans LAYERS_CONFIG.
"""

import os
import logging
import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import shape
from config import DATA_DIR, PROCESSED_DIR, LAYERS_CONFIG, TARGET_CRS


def load_csv_to_gdf(layer_key):
    """
    Charge un fichier CSV pour une couche donnée et le convertit en GeoDataFrame.

    Parameters:
        layer_key (str): Clé identifiant la couche dans LAYERS_CONFIG.

    Returns:
        GeoDataFrame: Contenant les données de la couche avec une colonne 'geometry'
        et un identifiant unique pour chaque entité.
    """
    config = LAYERS_CONFIG[layer_key]
    file_path = os.path.join(DATA_DIR, config["csv_file"])
    logging.info(f"Loading CSV for layer '{layer_key}' from {file_path}...")
    df = pd.read_csv(file_path, delimiter=";", encoding="utf-8", engine="python")
    geometries = []
    for geojson_str in df["geo_shape"]:
        try:
            geo_dict = json.loads(geojson_str)
            geometries.append(shape(geo_dict))
        except Exception:
            geometries.append(None)
    df["geometry"] = geometries
    df = df[df["geometry"].notnull()].copy()
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").to_crs(TARGET_CRS)
    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    return gdf


def load_geojson(layer_key):
    """
    Charge un fichier GeoJSON pour une couche donnée et le retourne sous forme de GeoDataFrame.

    Parameters:
        layer_key (str): Clé identifiant la couche dans LAYERS_CONFIG.

    Returns:
        GeoDataFrame or None: GeoDataFrame contenant les données chargées ou None en cas d'erreur.
    """
    config = LAYERS_CONFIG[layer_key]
    file_path = os.path.join(PROCESSED_DIR, config["geojson_file"])
    logging.info(f"Loading GeoJSON for layer '{layer_key}' from {file_path}...")
    try:
        gdf = gpd.read_file(file_path)
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf["source_layer"] = layer_key
        return gdf
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None


def load_all_layers(use_csv=False):
    """
    Charge toutes les couches définies dans LAYERS_CONFIG.

    Parameters:
        use_csv (bool): Si True, les données sont chargées à partir des CSV, sinon à partir des fichiers GeoJSON.

    Returns:
        dict: Dictionnaire dont les clés sont les noms de couche et les valeurs les GeoDataFrames correspondants.
    """
    layers = {}
    for layer_key in LAYERS_CONFIG:
        if use_csv:
            gdf = load_csv_to_gdf(layer_key)
        else:
            gdf = load_geojson(layer_key)
        if gdf is not None:
            layers[layer_key] = gdf
    return layers
