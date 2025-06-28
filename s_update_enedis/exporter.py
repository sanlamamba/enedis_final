"""Export processed data to Google Cloud Storage."""

import json
import logging
from typing import Dict
import geopandas as gpd
from google.cloud import storage
import io

from config import CLOUD_BUCKET_NAME, GCP_PROJECT_ID

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
