#!/usr/bin/env python3
"""
Module de traitement des données CSV et du calcul des connexions spatiales.

Ce module permet de charger les données CSV pour chaque couche, de calculer les connexions
spatiales entre entités, puis de sauvegarder le résultat sous forme de fichiers GeoJSON.
"""

import os
import logging
import geopandas as gpd
import pandas as pd
from config import PROCESSED_DIR, LAYERS_CONFIG
from loader import load_csv_to_gdf
from connections import find_connections
import json
from shapely.geometry import shape


def compute_connections(layers):
    """
    Reprojette les GeoDataFrames en EPSG:4326, les concatène pour créer un GeoDataFrame global,
    et calcule les connexions spatiales pour chaque couche.

    Parameters:
        layers (dict): Dictionnaire des GeoDataFrames pour chaque couche.

    Returns:
        dict: Dictionnaire mis à jour associant chaque couche à un GeoDataFrame enrichi avec
              les colonnes "connections", "start_connections" et "end_connections".
    """
    reprojected_layers = {}
    for layer_key, gdf in layers.items():
        if "source_layer" not in gdf.columns:
            gdf["source_layer"] = layer_key
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")
        reprojected_layers[layer_key] = gdf

    all_features = gpd.GeoDataFrame(
        pd.concat(list(reprojected_layers.values()), ignore_index=True), crs="EPSG:4326"
    )
    updated_layers = {}
    for layer_key, gdf in layers.items():
        cfg = LAYERS_CONFIG.get(layer_key, {})
        exclude = cfg.get("exclude_connections", [])
        priority = cfg.get("priority_connections", None)
        mono = cfg.get("mono_connection_per_endpoint", False)
        radius = cfg.get("radius", 3)
        updated_gdf = find_connections(
            gdf, all_features, radius, exclude, priority, mono
        )
        updated_layers[layer_key] = updated_gdf
    return updated_layers


def save_updated_layers(updated_layers):
    """
    Sauvegarde en GeoJSON chaque GeoDataFrame mis à jour dans le répertoire de sortie.

    Parameters:
        updated_layers (dict): Dictionnaire des GeoDataFrames mis à jour.
    """
    if not os.path.exists(PROCESSED_DIR):
        os.makedirs(PROCESSED_DIR)
    for layer_key, gdf in updated_layers.items():
        output_path = os.path.join(
            PROCESSED_DIR, LAYERS_CONFIG[layer_key]["geojson_file"]
        )
        try:
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(
                f"Saved updated GeoJSON for layer '{layer_key}' to {output_path}"
            )
        except Exception as e:
            logging.error(f"Error saving {output_path}: {e}")


def save_geojson(gdf, layer_key):
    """
    Sauvegarde un GeoDataFrame en tant que fichier GeoJSON pour une couche donnée.

    Parameters:
        gdf (GeoDataFrame): Ensemble de données à sauvegarder.
        layer_key (str): Clé de la couche dans LAYERS_CONFIG pour déterminer le nom du fichier.
    """
    output_path = os.path.join(PROCESSED_DIR, LAYERS_CONFIG[layer_key]["geojson_file"])
    try:
        gdf.to_file(output_path, driver="GeoJSON")
        logging.info(f"Saved GeoJSON for layer '{layer_key}' to {output_path}.")
    except Exception as e:
        logging.error(f"Error saving {output_path}: {e}")


def process_csv_layers():
    """
    Charge les données CSV pour chaque couche, les convertit en GeoDataFrame,
    sauvegarde chaque couche au format GeoJSON et retourne le dictionnaire
    des GeoDataFrames.

    Returns:
        dict: Dictionnaire associant chaque clé de couche à son GeoDataFrame correspondant.
    """
    layers = {}
    for layer_key in LAYERS_CONFIG:
        logging.info(f"Processing layer '{layer_key}' from CSV...")
        gdf = load_csv_to_gdf(layer_key)
        layers[layer_key] = gdf
        save_geojson(gdf, layer_key)
    return layers
