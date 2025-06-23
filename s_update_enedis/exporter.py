"""Export processed data to various formats."""

import logging
from typing import Dict
import geopandas as gpd

from config import OUTPUT_DIR, LAYERS

logger = logging.getLogger(__name__)


def save_geojson(gdf: gpd.GeoDataFrame, layer_key: str) -> str:
    """Save GeoDataFrame as GeoJSON."""
    gdf_wgs84 = gdf.to_crs("EPSG:4326")

    output_path = OUTPUT_DIR / f"{layer_key}.geojson"
    gdf_wgs84.to_file(output_path, driver="GeoJSON")

    return str(output_path)


def save_all_layers(layers: Dict[str, gpd.GeoDataFrame]):
    """Save all processed layers."""
    logger.info("Saving processed layers...")

    for layer_key, gdf in layers.items():
        try:
            path = save_geojson(gdf, layer_key)
            logger.info(f"Saved {layer_key} to {path}")
        except Exception as e:
            logger.error(f"Failed to save {layer_key}: {e}")

    try:
        combined = gpd.GeoDataFrame(
            pd.concat(layers.values(), ignore_index=True),
            crs=list(layers.values())[0].crs,
        )
        combined_path = save_geojson(combined, "combined_network")
        logger.info(f"Saved combined network to {combined_path}")
    except Exception as e:
        logger.error(f"Failed to save combined network: {e}")


def export_statistics(layers: Dict[str, gpd.GeoDataFrame]):
    """Export network statistics."""
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

    import json

    stats_path = OUTPUT_DIR / "network_statistics.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Exported statistics to {stats_path}")

    print("\nNetwork Statistics Summary:")
    print("-" * 50)
    for layer_key, layer_stats in stats.items():
        config = LAYERS[layer_key]
        print(f"\n{config.name}:")
        print(f"  Features: {layer_stats['feature_count']}")
        print(f"  Total connections: {layer_stats['total_connections']}")
        print(f"  Average connections: {layer_stats['avg_connections']:.2f}")


import pandas as pd
