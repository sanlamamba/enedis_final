#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main module of the Enedis pipeline.

This module orchestrates the CSV processing steps, spatial connection calculations,
and visualization generation.
"""

import os
import time
import logging
import argparse
from typing import Dict, Optional, List
import traceback

import geopandas as gpd

from process import process_csv_layers, save_updated_layers, validate_layer_data
from visualize import (
    create_folium_map,
    create_pyvis_network,
    generate_all_visualizations,
)
from connections import compute_connections
from chunked_processor import compute_connections_in_chunks
from bigquery_utils import upload_layers_to_bigquery
from config import PROCESSED_DIR, USE_CLOUD_STORAGE, LAYERS_CONFIG
from utils import timed


class EnedisPipeline:
    """Main pipeline class for the Enedis data processing workflow."""

    def __init__(
        self,
        skip_visualizations: bool = False,
        skip_bigquery: bool = False,
        use_chunked_processing: bool = False,
        chunk_size: int = 5000,
        available_memory_gb: float = 8.0,
    ):
        """
        Initialize the pipeline.

        Args:
            skip_visualizations: If True, skip visualization generation
            skip_bigquery: If True, skip BigQuery uploads
            use_chunked_processing: If True, use chunked processing for large datasets
            chunk_size: Maximum features per processing chunk
            available_memory_gb: Available memory in GB for optimizations
        """
        self.skip_visualizations = skip_visualizations
        self.skip_bigquery = skip_bigquery
        self.use_chunked_processing = use_chunked_processing
        self.chunk_size = chunk_size
        self.available_memory_gb = available_memory_gb
        self.start_time = None
        self.layers = {}
        self.updated_layers = {}

    def log_step(self, step_name: str) -> None:
        """Log the beginning of a pipeline step."""
        logging.info(f"=== Step: {step_name} ===")

    def log_performance(self, step_name: str, start_time: float) -> None:
        """Log the performance of a completed step."""
        elapsed = time.time() - start_time
        logging.info(f"=== Completed {step_name} in {elapsed:.2f} seconds ===")

    @timed
    def process_csv(self) -> Dict[str, gpd.GeoDataFrame]:
        """Process CSV files and return layers."""
        self.log_step("Processing CSV files")
        return process_csv_layers()

    @timed
    def calculate_connections(
        self, layers: Dict[str, gpd.GeoDataFrame]
    ) -> Dict[str, gpd.GeoDataFrame]:
        """Calculate spatial connections between entities."""
        if self.use_chunked_processing:
            self.log_step("Calculating spatial connections with chunked processing")

            # Determine overlap buffer - use 2x the maximum connection radius
            max_radius = max(
                [getattr(cfg, "radius", 3) for cfg in LAYERS_CONFIG.values()]
            )
            overlap_buffer = max_radius * 2

            return compute_connections_in_chunks(
                layers, chunk_size=self.chunk_size, overlap_buffer=overlap_buffer
            )
        else:
            self.log_step("Calculating spatial connections")
            return compute_connections(layers)

    @timed
    def optimize_network(
        self, layers: Dict[str, gpd.GeoDataFrame]
    ) -> Dict[str, gpd.GeoDataFrame]:
        """Optimize connection network for better visualization."""
        self.log_step("Optimizing connection network")
        # Note: You could implement chunked optimization here if needed
        from connections import optimize_connections

        return optimize_connections(layers)

    @timed
    def save_layers(self, layers: Dict[str, gpd.GeoDataFrame]) -> List[str]:
        """Save processed layers to disk/cloud."""
        self.log_step("Saving processed layers")
        return save_updated_layers(layers)

    @timed
    def upload_to_bigquery(self, layers: Dict[str, gpd.GeoDataFrame]) -> Dict[str, int]:
        """Upload layers to BigQuery."""
        if self.skip_bigquery:
            logging.info("Skipping BigQuery upload (--skip-bigquery flag set)")
            return {}

        self.log_step("Uploading to BigQuery")
        return upload_layers_to_bigquery(layers)

    @timed
    def create_visualizations(
        self, layers: Dict[str, gpd.GeoDataFrame]
    ) -> Dict[str, str]:
        """Generate all visualizations."""
        if self.skip_visualizations:
            logging.info("Skipping visualizations (--skip-visualizations flag set)")
            return {}

        self.log_step("Generating visualizations")
        return generate_all_visualizations(layers)

    def run(self) -> bool:
        """
        Run the complete pipeline.

        Returns:
            True if successful, False otherwise
        """
        self.start_time = time.time()
        logging.info("=== Enedis Pipeline Started ===")

        if self.use_chunked_processing:
            logging.info(f"Using chunked processing with chunk size: {self.chunk_size}")

        try:
            # Step 1: Process CSV files
            self.layers = self.process_csv()

            # Validate data quality
            validation_results = validate_layer_data(self.layers)
            has_critical_issues = any(
                len(issues) > 0 for issues in validation_results.values()
            )
            if has_critical_issues:
                logging.warning(
                    "Data validation found issues, but continuing with pipeline"
                )

            # Step 2: Calculate spatial connections
            self.updated_layers = self.calculate_connections(self.layers)

            # Step 3: Save updated layers
            self.save_layers(self.updated_layers)

            # Step 4: Upload to BigQuery
            bigquery_results = self.upload_to_bigquery(self.updated_layers)

            # Step 5: Generate visualizations
            visualization_outputs = self.create_visualizations(self.updated_layers)

            # Log summary
            elapsed = time.time() - self.start_time
            logging.info(
                f"=== Pipeline completed successfully in {elapsed:.2f} seconds ==="
            )

            if not self.skip_bigquery and bigquery_results:
                logging.info("BigQuery upload summary:")
                for layer, count in bigquery_results.items():
                    logging.info(f"  - {layer}: {count} rows")

            if not self.skip_visualizations and visualization_outputs:
                logging.info("Visualization outputs:")
                for viz_type, output_path in visualization_outputs.items():
                    logging.info(f"  - {viz_type}: {output_path}")

            return True

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            logging.error(traceback.format_exc())
            elapsed = time.time() - self.start_time
            logging.info(f"=== Pipeline failed after {elapsed:.2f} seconds ===")
            return False


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Enedis data processing pipeline")
    parser.add_argument(
        "--skip-visualizations",
        action="store_true",
        help="Skip generating visualizations",
    )
    parser.add_argument(
        "--skip-bigquery", action="store_true", help="Skip uploading to BigQuery"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set the logging level",
    )

    # Add memory management options
    parser.add_argument(
        "--use-chunks",
        action="store_true",
        help="Use chunked processing for large datasets to reduce memory usage",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of features to process per chunk (default: 5000)",
    )
    parser.add_argument(
        "--memory",
        type=float,
        default=8.0,
        help="Available memory in GB (used to optimize processing, default: 8.0)",
    )

    return parser.parse_args()


def setup_logging(log_level: str) -> None:
    """
    Configure logging settings.

    Args:
        log_level: Logging level to use
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(PROCESSED_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Set up file handler with timestamp in filename
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_file = os.path.join(logs_dir, f"pipeline-{timestamp}.log")

    handlers = [
        logging.StreamHandler(),  # Console handler
        logging.FileHandler(log_file),  # File handler
    ]

    # Configure logging
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        handlers=handlers,
    )

    logging.info(f"Logging initialized. Log file: {log_file}")


def update_geojson_enedis():
    """
    Main entry point for the Enedis data processing pipeline.
    """
    # Parse command line arguments
    args = parse_arguments()

    # Set up logging
    setup_logging(args.log_level)

    # Initialize and run the pipeline
    pipeline = EnedisPipeline(
        skip_visualizations=args.skip_visualizations,
        skip_bigquery=args.skip_bigquery,
        use_chunked_processing=args.use_chunks,
        chunk_size=args.chunk_size,
        available_memory_gb=args.memory,
    )

    success = pipeline.run()

    # Exit with appropriate code
    exit_code = 0 if success else 1
    return exit_code


if __name__ == "__main__":
    # Set environment variable to indicate running as main
    os.environ["RUNNING_AS_MAIN"] = "true"

    # Run the pipeline and get exit code
    exit_code = update_geojson_enedis()

    # Exit with code
    exit(exit_code)
