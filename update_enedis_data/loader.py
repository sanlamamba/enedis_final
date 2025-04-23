#!/usr/bin/env python3
"""
Module de chargement des données pour le projet Enedis.

Ce module fournit des fonctions pour charger les données depuis des fichiers CSV ou GeoJSON.
Les données CSV sont converties en GeoDataFrame en extrayant la géométrie depuis le champ "geo_shape".
Chaque couche est définie dans LAYERS_CONFIG.
"""

import os
import logging
import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import shape
from config import (
    DATA_DIR,
    PROCESSED_DIR,
    LAYERS_CONFIG,
    TARGET_CRS,
    USE_CLOUD_STORAGE,
    CLOUD_BUCKET_NAME,
    CLOUD_DATA_DIR,
)
from cloud_storage_utils import read_text_from_cloud, download_blob_to_temp_file


def load_csv_to_gdf(layer_key):
    """
    Charge un fichier CSV pour une couche donnée et le convertit en GeoDataFrame.

    Si l'option cloud est activée, le fichier est récupéré depuis le bucket cloud.
    En cas d'erreur, l'exception est levée.

    Parameters:
        layer_key (str): Clé identifiant la couche dans LAYERS_CONFIG.

    Returns:
        GeoDataFrame
    """
    config = LAYERS_CONFIG[layer_key]
    csv_filename = config["csv_file"]

    try:
        if USE_CLOUD_STORAGE:
            logging.info(
                f"Loading CSV for layer '{layer_key}' from cloud bucket '{CLOUD_BUCKET_NAME}'..."
            )
            cloud_csv_path = f"{CLOUD_DATA_DIR}/{csv_filename}"
            temp_file_path = download_blob_to_temp_file(
                CLOUD_BUCKET_NAME, cloud_csv_path
            )
            df = pd.read_csv(
                temp_file_path, delimiter=";", encoding="utf-8", engine="python"
            )
            os.remove(temp_file_path)
        else:
            file_path = os.path.join(DATA_DIR, csv_filename)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found: {file_path}")
            logging.info(f"Loading CSV for layer '{layer_key}' from {file_path}...")
            df = pd.read_csv(
                file_path, delimiter=";", encoding="utf-8", engine="python"
            )
    except Exception as e:
        logging.error(f"Failed to load CSV for layer '{layer_key}': {e}")
        raise

    geometries = []
    for geojson_str in df["geo_shape"]:
        try:
            geo_dict = json.loads(geojson_str)
            geometries.append(shape(geo_dict))
        except Exception:
            geometries.append(None)

    df["geometry"] = geometries
    df = df[df["geometry"].notnull()].copy()

    if df.empty:
        raise ValueError(f"No valid geometries found in CSV for layer '{layer_key}'")

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326").to_crs(TARGET_CRS)
    gdf["id"] = [f"{layer_key}_{i}" for i in range(1, len(gdf) + 1)]
    return gdf


def load_geojson(layer_key):
    """
    Charge un fichier GeoJSON pour une couche donnée et retourne un GeoDataFrame.

    Parameters:
        layer_key (str): Clé identifiant la couche dans LAYERS_CONFIG.

    Returns:
        GeoDataFrame
    """
    config = LAYERS_CONFIG[layer_key]
    file_path = os.path.join(PROCESSED_DIR, config["geojson_file"])

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"GeoJSON not found: {file_path}")

        logging.info(f"Loading GeoJSON for layer '{layer_key}' from {file_path}...")
        gdf = gpd.read_file(file_path)
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        gdf["source_layer"] = layer_key
        return gdf

    except Exception as e:
        logging.error(f"Failed to load GeoJSON for layer '{layer_key}': {e}")
        raise


def load_all_layers(use_csv=False):
    """
    Charge toutes les couches définies dans LAYERS_CONFIG.
    S'arrête immédiatement si une couche échoue à se charger.

    Parameters:
        use_csv (bool): Si True, les couches sont chargées depuis les CSV, sinon depuis les GeoJSON.

    Returns:
        dict: Clés = noms de couches, valeurs = GeoDataFrames.
    """
    layers = {}
    for layer_key in LAYERS_CONFIG:
        if use_csv:
            gdf = load_csv_to_gdf(layer_key)
        else:
            gdf = load_geojson(layer_key)

        layers[layer_key] = gdf

    return layers
