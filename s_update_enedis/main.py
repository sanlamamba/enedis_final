"""Optimized pipeline for electrical grid connection processing."""

import logging
import time
import os
import psutil
from datetime import datetime

from loader import load_all_layers_from_cloud
from loader_local import load_all_layers_from_local
from connections import process_all_connections
from exporter import save_all_layers_to_cloud, export_statistics_to_cloud
from exporter_local import save_all_layers_to_local, export_statistics_to_local

log_file_path = os.path.join(os.path.dirname(__file__), "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def format_memory_usage(bytes_used):
    """Format memory usage in human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_used < 1024.0:
            return f"{bytes_used:.2f} {unit}"
        bytes_used /= 1024.0
    return f"{bytes_used:.2f} TB"


def get_memory_info():
    """Get current memory usage information."""
    process = psutil.Process()
    memory_info = process.memory_info()
    return {
        "rss": memory_info.rss,  # Resident Set Size
        "vms": memory_info.vms,  # Virtual Memory Size
        "percent": process.memory_percent(),
    }


def print_performance_stats(stats):
    """Print performance statistics in a nice format."""
    print("\n" + "=" * 60)
    print("📊 PERFORMANCE STATISTICS")
    print("=" * 60)
    print(f"🕐 Start Time: {stats['start_time']}")
    print(f"🏁 End Time: {stats['end_time']}")
    print(f"⏱️  Total Duration: {stats['total_duration']:.2f} seconds")
    print("\n📈 TIMING BREAKDOWN:")
    for step, duration in stats["step_times"].items():
        print(f"   {step}: {duration:.2f}s")

    print(f"\n💾 MEMORY USAGE:")
    print(f"   Peak Memory: {format_memory_usage(stats['peak_memory'])}")
    print(f"   Final Memory: {format_memory_usage(stats['final_memory'])}")
    print(f"   Memory Usage: {stats['memory_percent']:.1f}%")

    print(f"\n📋 DATA STATISTICS:")
    print(f"   Layers Loaded: {stats['layers_count']}")
    print(f"   Total Features: {stats['total_features']:,}")
    print(f"   Total Connections: {stats['total_connections']:,}")
    print(f"   Avg Connections/Feature: {stats['avg_connections_per_feature']:.2f}")

    print("=" * 60)
    print("✅ Processing Complete!")
    print("=" * 60 + "\n")


def main():
    """Run the optimized electrical grid processing pipeline."""
    start_time = time.time()
    start_datetime = datetime.now()

    # Initialize performance tracking
    performance_stats = {
        "start_time": start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "step_times": {},
        "peak_memory": 0,
        "final_memory": 0,
        "memory_percent": 0,
    }

    # Get initial memory usage
    initial_memory = get_memory_info()
    performance_stats["peak_memory"] = initial_memory["rss"]

    try:
        logger.info("🚀 Starting optimized electrical grid processing pipeline")

        # Step 1: Load data from Google Cloud Storage
        step_start = time.time()
        logger.info("=== Loading electrical grid data from Google Cloud ===")
        layers = load_all_layers_from_local()

        # Track memory usage
        current_memory = get_memory_info()
        performance_stats["peak_memory"] = max(
            performance_stats["peak_memory"], current_memory["rss"]
        )

        # Calculate data statistics
        total_features = sum(len(gdf) for gdf in layers.values())
        performance_stats["layers_count"] = len(layers)
        performance_stats["total_features"] = total_features
        performance_stats["step_times"]["Data Loading"] = time.time() - step_start

        logger.info(
            f"Loaded {len(layers)} layers with {total_features:,} total features"
        )

        # Step 2: Calculate network connections
        step_start = time.time()
        logger.info("=== Calculating network connections ===")
        connected_layers = process_all_connections(layers)

        # Track memory usage
        current_memory = get_memory_info()
        performance_stats["peak_memory"] = max(
            performance_stats["peak_memory"], current_memory["rss"]
        )

        total_connections = sum(
            sum(len(feature["connections"]) for _, feature in gdf.iterrows())
            for gdf in connected_layers.values()
        )
        performance_stats["total_connections"] = total_connections
        performance_stats["avg_connections_per_feature"] = (
            total_connections / total_features if total_features > 0 else 0
        )
        performance_stats["step_times"]["Connection Processing"] = (
            time.time() - step_start
        )

        logger.info(f"Generated {total_connections:,} total connections")

        # Step 3: Save results to Google Cloud Storage in 'processed' directory
        step_start = time.time()
        logger.info("=== Saving results to Google Cloud Storage ===")
        save_all_layers_to_local(connected_layers)
        export_statistics_to_local(connected_layers)

        # Track memory usage
        current_memory = get_memory_info()
        performance_stats["peak_memory"] = max(
            performance_stats["peak_memory"], current_memory["rss"]
        )
        performance_stats["step_times"]["Data Export"] = time.time() - step_start

        # Final performance calculations
        end_datetime = datetime.now()
        elapsed = time.time() - start_time
        final_memory = get_memory_info()

        performance_stats.update(
            {
                "end_time": end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "total_duration": elapsed,
                "final_memory": final_memory["rss"],
                "memory_percent": final_memory["percent"],
            }
        )

        logger.info(f"✅ Pipeline completed successfully in {elapsed:.2f} seconds")

        # Print detailed performance statistics
        print_performance_stats(performance_stats)

        return True

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
