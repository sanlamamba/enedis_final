"""Export processed data to local folder."""

import json
import logging
import os
from typing import Dict
import geopandas as gpd
from config import LOCAL_OUTPUT_PATH

logger = logging.getLogger(__name__)


def save_to_local_storage(
    gdf: gpd.GeoDataFrame, layer_key: str, output_dir: str = "processed"
) -> str:
    """Save GeoDataFrame as GeoJSON to local folder."""
    # Create output directory if it doesn't exist
    full_output_dir = os.path.join(LOCAL_OUTPUT_PATH, output_dir)
    os.makedirs(full_output_dir, exist_ok=True)

    file_path = os.path.join(full_output_dir, f"{layer_key}.geojson")

    # Convert to WGS84 if needed (using a view when possible)
    if gdf.crs and gdf.crs != "EPSG:4326":
        gdf_wgs84 = gdf.to_crs("EPSG:4326")
    else:
        gdf_wgs84 = gdf

    # Use Fiona's streaming writer for memory efficiency
    import fiona
    from fiona.crs import from_epsg

    schema = {
        "geometry": gdf_wgs84.geometry.iloc[0].geom_type,
        "properties": {col: "str" for col in gdf_wgs84.columns if col != "geometry"},
    }

    # Stream write row by row to avoid loading entire JSON into memory
    with fiona.open(
        file_path, "w", driver="GeoJSON", crs=from_epsg(4326), schema=schema
    ) as output:
        for _, row in gdf_wgs84.iterrows():
            feature = {
                "geometry": row.geometry.__geo_interface__,
                "properties": {
                    col: str(row[col]) for col in gdf_wgs84.columns if col != "geometry"
                },
            }
            output.write(feature)

    logger.info(f"Saved {layer_key} to {file_path}")
    return file_path


def save_to_local_storage_old(
    gdf: gpd.GeoDataFrame, layer_key: str, output_dir: str = "processed"
) -> str:
    """Save GeoDataFrame as GeoJSON to local folder."""
    gdf_wgs84 = gdf.to_crs("EPSG:4326")
    geojson_str = gdf_wgs84.to_json()

    # Create output directory if it doesn't exist
    full_output_dir = os.path.join(LOCAL_OUTPUT_PATH, output_dir)
    os.makedirs(full_output_dir, exist_ok=True)

    # Save to local file
    file_path = os.path.join(full_output_dir, f"{layer_key}.geojson")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(geojson_str)

    logger.info(f"Saved {layer_key} to {file_path}")
    return file_path


def save_all_layers_to_local(layers: Dict[str, gpd.GeoDataFrame]):
    """Save all processed layers to local folder."""
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


def export_statistics_to_local(layers: Dict[str, gpd.GeoDataFrame]):
    """Export network statistics to local folder."""
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

    # Save statistics to local file
    output_dir = os.path.join(LOCAL_OUTPUT_PATH, "processed")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, "network_statistics.json")
    stats_json = json.dumps(stats, indent=2)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(stats_json)

    logger.info(f"Exported statistics to {file_path}")

    # Print summary
    print("\nNetwork Statistics Summary:")
    print("-" * 50)
    for layer_key, layer_stats in stats.items():
        print(f"\n{layer_key}:")
        print(f"  Features: {layer_stats['feature_count']}")
        print(f"  Total connections: {layer_stats['total_connections']}")
        print(f"  Average connections: {layer_stats['avg_connections']:.2f}")
