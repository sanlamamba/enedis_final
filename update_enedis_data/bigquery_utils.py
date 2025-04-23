#!/usr/bin/env python3
"""
Module d'upload dans BigQuery pour le projet Enedis.

Chaque GeoJSON est chargé dans une table distincte BigQuery nommée selon le fichier,
dans le dataset défini.
"""

import os
import json
import logging
import tempfile
from google.cloud import bigquery
from config import PROCESSED_DIR, LAYERS_CONFIG, BIGQUERY_TABLE_ID


def extract_record(feature):
    props = feature.get("properties", {})
    geom_obj = feature.get("geometry", {})

    node_ref = props.get("id")
    try:
        record_id = int(node_ref.split("_")[-1])
    except Exception as e:
        logging.error(f"Erreur ID int depuis {node_ref}: {e}")
        raise

    from shapely.geometry import shape

    try:
        geom_wkt = shape(geom_obj).wkt
    except Exception as e:
        logging.error(f"Erreur conversion géométrie: {e}")
        raise

    coordinates = (
        ",".join(str(c) for c in geom_obj.get("coordinates", []))
        if geom_obj.get("coordinates")
        else ""
    )
    geom_type = geom_obj.get("type", "Unknown")

    return {
        "id": record_id,
        "node_ref": node_ref,
        "code_commune": props.get("code_commune"),
        "nom_commune": props.get("nom_commune"),
        "code_epci": props.get("code_epci"),
        "nom_epci": props.get("nom_epci"),
        "code_departement": props.get("code_departement"),
        "nom_departement": props.get("nom_departement"),
        "code_region": props.get("code_region"),
        "nom_region": props.get("nom_region"),
        "connections": props.get("connections", []),
        "start_connections": props.get("start_connections", []),
        "end_connections": props.get("end_connections", []),
        "geometry": geom_wkt,
        "coordinates": coordinates,
        "type": geom_type,
    }


def upload_layers_to_bigquery(updated_layers):
    """
    Uploads each updated GeoJSON layer into its dedicated BigQuery table.
    """
    client = bigquery.Client()
    project, dataset = BIGQUERY_TABLE_ID.split(".")[0], BIGQUERY_TABLE_ID.split(".")[1]

    for layer_key in updated_layers:
        output_filename = LAYERS_CONFIG[layer_key]["geojson_file"]
        table_name = output_filename.replace(".geojson", "")
        full_table_id = f"{project}.{dataset}.{table_name}"

        file_path = os.path.join(PROCESSED_DIR, output_filename)
        logging.info(
            f"Lecture du GeoJSON '{file_path}' pour upload dans '{full_table_id}'..."
        )

        try:
            with open(file_path, "r") as f:
                geojson = json.load(f)
        except Exception as e:
            logging.error(f"Erreur lecture {file_path}: {e}")
            raise

        if "features" not in geojson:
            raise ValueError(f"Aucune feature trouvée dans {file_path}")

        records = []
        for feature in geojson["features"]:
            try:
                records.append(extract_record(feature))
            except Exception as e:
                logging.error(f"Erreur extraction record: {e}")
                raise

        if not records:
            logging.warning(f"Aucun enregistrement à charger pour {layer_key}")
            continue

        # Write to newline-delimited JSON
        with tempfile.NamedTemporaryFile(
            "w+", delete=False, suffix=".json"
        ) as tmp_file:
            for rec in records:
                tmp_file.write(json.dumps(rec) + "\n")
            temp_filename = tmp_file.name

        logging.info(f"{len(records)} enregistrements écrits dans {temp_filename}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("node_ref", "STRING"),
                bigquery.SchemaField("code_commune", "INTEGER"),
                bigquery.SchemaField("nom_commune", "STRING"),
                bigquery.SchemaField("code_epci", "INTEGER"),
                bigquery.SchemaField("nom_epci", "STRING"),
                bigquery.SchemaField("code_departement", "INTEGER"),
                bigquery.SchemaField("nom_departement", "STRING"),
                bigquery.SchemaField("code_region", "INTEGER"),
                bigquery.SchemaField("nom_region", "STRING"),
                bigquery.SchemaField("connections", "STRING", mode="REPEATED"),
                bigquery.SchemaField("start_connections", "STRING", mode="REPEATED"),
                bigquery.SchemaField("end_connections", "STRING", mode="REPEATED"),
                bigquery.SchemaField("geometry", "GEOGRAPHY"),
                bigquery.SchemaField("coordinates", "STRING"),
                bigquery.SchemaField("type", "STRING"),
            ],
        )

        try:
            with open(temp_filename, "rb") as source_file:
                load_job = client.load_table_from_file(
                    source_file, full_table_id, job_config=job_config
                )
            load_job.result()
            logging.info(f"{load_job.output_rows} lignes insérées dans {full_table_id}")
        except Exception as e:
            logging.error(f"Erreur chargement dans {full_table_id}: {e}")
            raise
        finally:
            os.remove(temp_filename)
