"""Main pipeline for processing electrical grid data."""

import logging
import time
from pathlib import Path

from loader import load_all_layers
from connections import process_all_connections
from exporter import save_all_layers, export_statistics
from visualizer import create_network_map, create_connection_graph

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Run the complete processing pipeline."""
    start_time = time.time()

    try:
        logger.info("=== Loading electrical grid data ===")
        layers = load_all_layers()

        logger.info("=== Calculating network connections ===")
        connected_layers = process_all_connections(layers)

        logger.info("=== Saving results ===")
        save_all_layers(connected_layers)
        export_statistics(connected_layers)

        logger.info("=== Creating visualizations ===")
        create_network_map(connected_layers)
        # create_connection_graph(connected_layers)

        elapsed = time.time() - start_time
        logger.info(f"=== Pipeline completed in {elapsed:.2f} seconds ===")

        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
