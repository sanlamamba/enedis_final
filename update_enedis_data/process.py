#!/usr/bin/env python3
"""
Module de traitement des données CSV et du calcul des connexions spatiales.

Ce module permet de charger les données CSV pour chaque couche, de calculer les connexions
spatiales entre entités, puis de sauvegarder le résultat sous forme de fichiers GeoJSON.
"""

import os
import logging
import tempfile
import geopandas as gpd
import pandas as pd
import zipfile
from datetime import datetime

from config import (
    PROCESSED_DIR,
    LAYERS_CONFIG,
    USE_CLOUD_STORAGE,
    CLOUD_BUCKET_NAME,
    SHOULD_EXPORT_GEOJSON,
    SHOULD_EXPORT_SHAPEFILE,
)
from loader import load_csv_to_gdf
from cloud_storage_utils import upload_file_to_cloud


def add_filedate(gdf):
    """
    Ajoute une colonne 'filedate' avec la date du jour au format YYYYMMDD.

    Parameters:
        gdf (GeoDataFrame): Données géographiques à enrichir.

    Returns:
        GeoDataFrame: Données avec colonne filedate ajoutée.
    """
    gdf["filedate"] = datetime.today().strftime("%Y%m%d")
    return gdf


def save_updated_layers(updated_layers):
    """
    Sauvegarde chaque GeoDataFrame mis à jour en GeoJSON.
    Si l'option cloud est activée (USE_CLOUD_STORAGE), le fichier est également téléversé dans le bucket cloud.
    La sauvegarde locale se fait dans le répertoire PROCESSED_DIR.

    Parameters:
        updated_layers (dict): Dictionnaire des GeoDataFrames mis à jour.
    """
    if not updated_layers:
        logging.info("Aucune couche à sauvegarder.")
        return

    for layer_key, gdf in updated_layers.items():
        try:
            gdf = add_filedate(gdf)
            output_filename = LAYERS_CONFIG[layer_key]["geojson_file"]
            output_path = os.path.join(PROCESSED_DIR, output_filename)

            if USE_CLOUD_STORAGE:
                logging.info(
                    f"Téléversement de la couche '{layer_key}' vers le cloud..."
                )
                with tempfile.NamedTemporaryFile(
                    "w+", delete=False, suffix=".geojson"
                ) as tmp:
                    temp_path = tmp.name
                gdf.to_file(temp_path, driver="GeoJSON")
                upload_file_to_cloud(temp_path, CLOUD_BUCKET_NAME, output_filename)
                os.remove(temp_path)
                logging.info(f"GeoJSON téléversé dans le cloud: {output_filename}")
            else:
                os.makedirs(PROCESSED_DIR, exist_ok=True)
                logging.info(f"Sauvegarde locale de la couche '{layer_key}'...")
                gdf.to_file(output_path, driver="GeoJSON")
                logging.info(f"GeoJSON sauvegardé localement à {output_path}")

        except Exception as e:
            logging.error(
                f"Erreur lors de la sauvegarde de la couche '{layer_key}': {e}"
            )
            raise e


def save_geojson(gdf, layer_key):
    """
    Sauvegarde un GeoDataFrame en tant que fichier GeoJSON pour une couche donnée.

    Parameters:
        gdf (GeoDataFrame): Ensemble de données à sauvegarder.
        layer_key (str): Clé de la couche dans LAYERS_CONFIG pour déterminer le nom du fichier.
    """
    gdf = add_filedate(gdf)
    output_filename = LAYERS_CONFIG[layer_key]["geojson_file"]

    if USE_CLOUD_STORAGE:
        logging.info(
            f"Saving GeoJSON for layer '{layer_key}' to Cloud Storage in bucket {CLOUD_BUCKET_NAME}..."
        )
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".geojson") as tmp:
            temp_path = tmp.name
        gdf.to_file(temp_path, driver="GeoJSON")
        upload_file_to_cloud(temp_path, CLOUD_BUCKET_NAME, output_filename)
        os.remove(temp_path)
        logging.info(
            f"Saved GeoJSON for layer '{layer_key}' to cloud as {output_filename}."
        )
    else:
        if not os.path.exists(PROCESSED_DIR):
            os.makedirs(PROCESSED_DIR)
        output_path = os.path.join(PROCESSED_DIR, output_filename)
        try:
            gdf.to_file(output_path, driver="GeoJSON")
            logging.info(
                f"Saved updated GeoJSON for layer '{layer_key}' to {output_path}"
            )
        except Exception as e:
            logging.error(f"Error saving {output_path}: {e}")


def save_shapefile(gdf, layer_key):
    """
    Sauvegarde un GeoDataFrame en tant que ShapeFile pour une couche donnée.

    Parameters:
        gdf (GeoDataFrame): Ensemble de données à sauvegarder.
        layer_key (str): Clé de la couche dans LAYERS_CONFIG utilisée pour déterminer le nom du fichier.
    """
    gdf = add_filedate(gdf)
    if USE_CLOUD_STORAGE:
        logging.info(
            f"Saving Shapefile for layer '{layer_key}' to Cloud Storage in bucket {CLOUD_BUCKET_NAME}..."
        )
        with tempfile.TemporaryDirectory() as tmpdirname:
            base_filename = layer_key
            shapefile_path = os.path.join(tmpdirname, base_filename + ".shp")

            gdf.to_file(shapefile_path, driver="ESRI Shapefile")
            zip_filename = LAYERS_CONFIG[layer_key].get(
                "shapefile_file", f"{layer_key}.zip"
            )
            zip_path = os.path.join(tmpdirname, zip_filename)

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for filename in os.listdir(tmpdirname):
                    if filename.startswith(base_filename):
                        full_path = os.path.join(tmpdirname, filename)
                        zipf.write(full_path, arcname=filename)

            upload_file_to_cloud(zip_path, CLOUD_BUCKET_NAME, zip_filename)
            logging.info(
                f"Saved Shapefile for layer '{layer_key}' to cloud as {zip_filename}."
            )
    else:
        if not os.path.exists(PROCESSED_DIR):
            os.makedirs(PROCESSED_DIR)
        shapefile_path = os.path.join(PROCESSED_DIR, layer_key + ".shp")
        try:
            gdf.to_file(shapefile_path, driver="ESRI Shapefile")
            logging.info(f"Saved Shapefile for layer '{layer_key}' to {shapefile_path}")
        except Exception as e:
            logging.error(f"Error saving shapefile at {shapefile_path}: {e}")


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
        if SHOULD_EXPORT_GEOJSON:
            save_geojson(gdf, layer_key)
        if SHOULD_EXPORT_SHAPEFILE:
            save_shapefile(gdf, layer_key)

    return layers
