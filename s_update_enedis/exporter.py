"""Export processed data to Google Cloud Storage or local files."""

import json
import logging
import os
from pathlib import Path
from typing import Dict
import geopandas as gpd
from google.cloud import storage
import io

from config import CLOUD_BUCKET_NAME, GCP_PROJECT_ID

# Local output configuration
LOCAL_OUTPUT_PATH = "../output"

logger = logging.getLogger(__name__)


def save_to_cloud_storage(
    gdf: gpd.GeoDataFrame, layer_key: str, output_dir: str = "processed"
) -> str:
    """Save GeoDataFrame as GeoJSON to Google Cloud Storage."""
    gdf_wgs84 = gdf.to_crs("EPSG:4326")

    geojson_str = gdf_wgs84.to_json()

    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(CLOUD_BUCKET_NAME)

    blob_path = f"{output_dir}/{layer_key}.geojson"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(geojson_str, content_type="application/json")

    cloud_path = f"gs://{CLOUD_BUCKET_NAME}/{blob_path}"
    logger.info(f"Saved {layer_key} to {cloud_path}")

    return cloud_path


def save_to_local_storage(
    gdf: gpd.GeoDataFrame, layer_key: str, output_dir: str = "processed"
) -> str:
    """Save GeoDataFrame as GeoJSON to local output folder."""
    # Convert to WGS84 for standard GeoJSON
    gdf_wgs84 = gdf.to_crs("EPSG:4326")

    # Create output directory if it doesn't exist
    local_output_dir = Path(LOCAL_OUTPUT_PATH) / output_dir
    local_output_dir.mkdir(parents=True, exist_ok=True)

    # Save as GeoJSON
    output_path = local_output_dir / f"{layer_key}.geojson"
    gdf_wgs84.to_file(output_path, driver="GeoJSON")

    logger.info(f"Saved {layer_key} to {output_path}")
    return str(output_path)


def save_all_layers_to_cloud(layers: Dict[str, gpd.GeoDataFrame]):
    """Save all processed layers to Google Cloud Storage."""
    logger.info("Saving processed layers to cloud storage...")

    for layer_key, gdf in layers.items():
        try:
            save_to_cloud_storage(gdf, layer_key)
        except Exception as e:
            logger.error(f"Failed to save {layer_key} to cloud: {e}")
            raise

    # Save combined network
    try:
        import pandas as pd

        combined = gpd.GeoDataFrame(
            pd.concat(layers.values(), ignore_index=True),
            crs=list(layers.values())[0].crs,
        )
        save_to_cloud_storage(combined, "combined_network")
    except Exception as e:
        logger.error(f"Failed to save combined network to cloud: {e}")


def save_all_layers_to_local(layers: Dict[str, gpd.GeoDataFrame]):
    """Save all processed layers to local output folder."""
    logger.info("Saving processed layers to local storage...")

    for layer_key, gdf in layers.items():
        try:
            save_to_local_storage(gdf, layer_key)
        except Exception as e:
            logger.error(f"Failed to save {layer_key} to local: {e}")
            raise

    # Save combined network
    try:
        import pandas as pd

        combined = gpd.GeoDataFrame(
            pd.concat(layers.values(), ignore_index=True),
            crs=list(layers.values())[0].crs,
        )
        save_to_local_storage(combined, "combined_network")
    except Exception as e:
        logger.error(f"Failed to save combined network to local: {e}")


def export_statistics_to_cloud(layers: Dict[str, gpd.GeoDataFrame]):
    """Export network statistics to Google Cloud Storage."""
    stats = {}

    for layer_key, gdf in layers.items():
        layer_stats = {
            "feature_count": len(gdf),
            "total_connections": sum(len(f["connections"]) for _, f in gdf.iterrows()),
            "avg_connections": (
                sum(len(f["connections"]) for _, f in gdf.iterrows()) / len(gdf)
                if len(gdf) > 0
                else 0
            ),
            "geometry_types": gdf.geometry.geom_type.value_counts().to_dict(),
        }
        stats[layer_key] = layer_stats

    # Save statistics to cloud
    client = storage.Client(project=GCP_PROJECT_ID)
    bucket = client.bucket(CLOUD_BUCKET_NAME)

    blob_path = "processed/network_statistics.json"
    blob = bucket.blob(blob_path)

    stats_json = json.dumps(stats, indent=2)
    blob.upload_from_string(stats_json, content_type="application/json")

    logger.info(f"Exported statistics to gs://{CLOUD_BUCKET_NAME}/{blob_path}")

    # Print summary
    print("\nNetwork Statistics Summary:")
    print("-" * 50)
    for layer_key, layer_stats in stats.items():
        print(f"\n{layer_key}:")
        print(f"  Features: {layer_stats['feature_count']}")
        print(f"  Total connections: {layer_stats['total_connections']}")
        print(f"  Average connections: {layer_stats['avg_connections']:.2f}")


def export_statistics_to_local(layers: Dict[str, gpd.GeoDataFrame]):
    """Export network statistics to local output folder."""
    stats = {}

    for layer_key, gdf in layers.items():
        layer_stats = {
            "feature_count": len(gdf),
            "total_connections": sum(len(f["connections"]) for _, f in gdf.iterrows()),
            "avg_connections": (
                sum(len(f["connections"]) for _, f in gdf.iterrows()) / len(gdf)
                if len(gdf) > 0
                else 0
            ),
            "geometry_types": gdf.geometry.geom_type.value_counts().to_dict(),
        }
        stats[layer_key] = layer_stats

    # Create output directory if it doesn't exist
    local_output_dir = Path(LOCAL_OUTPUT_PATH) / "processed"
    local_output_dir.mkdir(parents=True, exist_ok=True)

    # Save statistics to local file
    stats_path = local_output_dir / "network_statistics.json"
    stats_json = json.dumps(stats, indent=2)

    with open(stats_path, "w", encoding="utf-8") as f:
        f.write(stats_json)

    logger.info(f"Exported statistics to {stats_path}")

    # Print summary
    print("\nNetwork Statistics Summary:")
    print("-" * 50)
    for layer_key, layer_stats in stats.items():
        print(f"\n{layer_key}:")
        print(f"  Features: {layer_stats['feature_count']}")
        print(f"  Total connections: {layer_stats['total_connections']}")
        print(f"  Average connections: {layer_stats['avg_connections']:.2f}")


def save_all_layers(layers: Dict[str, gpd.GeoDataFrame], destination: str = "local"):
    """Save all processed layers to specified destination (local or cloud).

    Args:
        layers: Dictionary mapping layer keys to GeoDataFrames
        destination: Either 'local' or 'cloud' to specify output destination
    """
    if destination.lower() == "cloud":
        save_all_layers_to_cloud(layers)
    elif destination.lower() == "local":
        save_all_layers_to_local(layers)
    else:
        raise ValueError("Destination must be either 'local' or 'cloud'")


def export_statistics(layers: Dict[str, gpd.GeoDataFrame], destination: str = "local"):
    """Export network statistics to specified destination (local or cloud).

    Args:
        layers: Dictionary mapping layer keys to GeoDataFrames
        destination: Either 'local' or 'cloud' to specify output destination
    """
    if destination.lower() == "cloud":
        export_statistics_to_cloud(layers)
    elif destination.lower() == "local":
        export_statistics_to_local(layers)
    else:
        raise ValueError("Destination must be either 'local' or 'cloud'")


def save_layer(
    gdf: gpd.GeoDataFrame,
    layer_key: str,
    destination: str = "local",
    output_dir: str = "processed",
) -> str:
    """Save a single layer to specified destination (local or cloud).

    Args:
        gdf: GeoDataFrame to save
        layer_key: Key identifying the layer
        destination: Either 'local' or 'cloud' to specify output destination
        output_dir: Output directory name

    Returns:
        Path or URL where the file was saved
    """
    if destination.lower() == "cloud":
        return save_to_cloud_storage(gdf, layer_key, output_dir)
    elif destination.lower() == "local":
        return save_to_local_storage(gdf, layer_key, output_dir)
    else:
        raise ValueError("Destination must be either 'local' or 'cloud'")
