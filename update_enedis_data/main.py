# main.py
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Module principal du pipeline Enedis.

Ce module orchestre les étapes du traitement des CSV, du calcul des connexions spatiales
et de la génération des visualisations.
"""

import time
import logging
from process import process_csv_layers, save_updated_layers
from visualize import create_folium_map, create_pyvis_network
from connections import compute_connections
from bigquery_utils import upload_layers_to_bigquery


def update_geojson_enedis():
    start_time = time.time()
    logging.info("=== Pipeline démarré ===")

    # Étape 1 : Traitement des fichiers CSV
    logging.info("Étape 1 : Traitement des CSV")
    layers = process_csv_layers()

    logging.info("Étape 2 : Calcul des connexions spatiales")
    updated_layers = compute_connections(layers)
    save_updated_layers(updated_layers)

    upload_layers_to_bigquery(updated_layers)

    # Étape 3 : Génération des visualisations
    logging.info("Étape 3 : Génération des visualisations")
    # create_folium_map()
    # create_pyvis_network()

    elapsed = time.time() - start_time
    logging.info(f"=== Pipeline terminé en {elapsed:.2f} secondes ===")


update_geojson_enedis()
