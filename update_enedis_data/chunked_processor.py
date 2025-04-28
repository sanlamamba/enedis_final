#!/usr/bin/env python3
"""
Chunked processing module for the Enedis project.

This module provides functions for processing large datasets in chunks
while preserving network connections between entities.
"""

import os
import json
import logging
import tempfile
from typing import Dict, List, Set, Iterator, Tuple, Any, Optional
from pathlib import Path
import itertools
import numpy as np

import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, Point, LineString, box
import rtree
from tqdm import tqdm

from config import LAYERS_CONFIG, TARGET_CRS
from utils import timed, retry
from connections import find_connections


class ChunkedProcessor:
    """Process large GeoJSON and spatial data in memory-efficient chunks."""

    def __init__(self, chunk_size: int = 5000, overlap_buffer: float = 10.0):
        """
        Initialize the chunked processor.

        Args:
            chunk_size: Maximum features per chunk
            overlap_buffer: Buffer distance (meters) to ensure connections between chunks
        """
        self.chunk_size = chunk_size
        self.overlap_buffer = overlap_buffer
        self.connection_index = {}  # Global index of feature IDs to their connections
        self.processed_features = (
            set()
        )  # Track processed feature IDs to avoid duplicates

    @timed
    def process_layer_in_chunks(
        self, layer_key: str, all_layers: Dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        """
        Process a layer in chunks while ensuring all connections are maintained.

        This implementation directly integrates with your existing code structure
        and reuses your connection calculation logic.

        Args:
            layer_key: Key identifying the layer in LAYERS_CONFIG
            all_layers: Dictionary of all layer GeoDataFrames for connection calculation

        Returns:
            Processed GeoDataFrame with all connections preserved
        """
        if layer_key not in all_layers:
            logging.error(f"Layer {layer_key} not found in available layers")
            return gpd.GeoDataFrame()

        full_gdf = all_layers[layer_key]

        # Skip small datasets - no need for chunking
        if len(full_gdf) <= self.chunk_size:
            logging.info(
                f"Layer {layer_key} has {len(full_gdf)} features - processing in one go"
            )
            return full_gdf

        logging.info(
            f"Processing layer {layer_key} with {len(full_gdf)} features in chunks"
        )

        # Ensure CRS is set
        if full_gdf.crs is None:
            full_gdf.set_crs(epsg=4326, inplace=True)

        # Convert to projected CRS for accurate spatial operations
        if full_gdf.crs != "EPSG:3857":
            full_gdf_proj = full_gdf.to_crs(epsg=3857)
        else:
            full_gdf_proj = full_gdf

        # Create spatial chunks based on geographic distribution
        chunk_bounds = self._create_spatial_chunks(full_gdf_proj)
        logging.info(f"Created {len(chunk_bounds)} spatial chunks for processing")

        processed_chunks = []

        # Get config for the layer
        cfg = LAYERS_CONFIG.get(layer_key)
        exclude_connections = getattr(cfg, "exclude_connections", [])
        priority_connections = getattr(cfg, "priority_connections", None)
        mono_connection = getattr(cfg, "mono_connection_per_endpoint", False)
        radius = getattr(cfg, "radius", 3)

        # Process each chunk with connections
        for i, bounds in enumerate(chunk_bounds):
            logging.info(
                f"Processing chunk {i+1}/{len(chunk_bounds)} for layer {layer_key}"
            )

            # Extract features within this chunk (with buffer)
            chunk_gdf = self._extract_chunk(full_gdf_proj, bounds)

            if chunk_gdf.empty:
                logging.warning(f"Chunk {i+1} has no features - skipping")
                continue

            # For connection calculation, we need all layers in the projected CRS
            all_layers_proj = {}
            for k, gdf in all_layers.items():
                if gdf.crs != "EPSG:3857":
                    all_layers_proj[k] = gdf.to_crs(epsg=3857)
                else:
                    all_layers_proj[k] = gdf

            # Add source_layer if not present
            if "source_layer" not in chunk_gdf.columns:
                chunk_gdf["source_layer"] = layer_key

            # Calculate connections for this chunk using your existing function
            try:
                # Create a combined GeoDataFrame with all features
                all_features_combined = pd.concat(
                    list(all_layers_proj.values()), ignore_index=True
                )
                all_features_combined = gpd.GeoDataFrame(
                    all_features_combined, geometry="geometry", crs="EPSG:3857"
                )

                # Ensure we have spatial index
                if (
                    not hasattr(all_features_combined, "sindex")
                    or all_features_combined.sindex is None
                ):
                    all_features_combined.sindex

                # Process chunk using existing find_connections function
                chunk_with_connections = find_connections(
                    chunk_gdf,
                    all_features_combined,
                    radius,
                    exclude_connections,
                    priority_connections,
                    mono_connection,
                )

                # Convert back to WGS84
                chunk_with_connections = chunk_with_connections.to_crs(epsg=4326)

                # Add to processed chunks
                processed_chunks.append(chunk_with_connections)
                logging.info(
                    f"Processed chunk {i+1} with {len(chunk_with_connections)} features"
                )

            except Exception as e:
                logging.error(f"Error processing chunk {i+1}: {e}")
                raise

        if not processed_chunks:
            logging.warning(
                f"No chunks were successfully processed for layer {layer_key}"
            )
            return gpd.GeoDataFrame()

        # Combine all processed chunks and remove duplicates
        combined_gdf = pd.concat(processed_chunks, ignore_index=True)
        combined_gdf = combined_gdf.drop_duplicates(subset=["id"])

        logging.info(
            f"Successfully processed {len(combined_gdf)} features for layer {layer_key}"
        )
        return combined_gdf

    def _create_spatial_chunks(
        self, gdf: gpd.GeoDataFrame
    ) -> List[Tuple[float, float, float, float]]:
        """
        Create spatial chunks based on feature distribution.

        Args:
            gdf: GeoDataFrame to chunk

        Returns:
            List of chunk bounds (minx, miny, maxx, maxy)
        """
        # Get total bounds
        minx, miny, maxx, maxy = gdf.total_bounds

        # Determine number of chunks based on data size
        num_chunks = max(1, int(np.ceil(len(gdf) / self.chunk_size)))

        # Calculate grid dimensions
        grid_size = max(1, int(np.ceil(np.sqrt(num_chunks))))

        # Calculate cell sizes
        x_step = (maxx - minx) / grid_size
        y_step = (maxy - miny) / grid_size

        # Generate grid cells with overlap
        chunk_bounds = []
        for i in range(grid_size):
            for j in range(grid_size):
                cell_minx = minx + (i * x_step) - (self.overlap_buffer if i > 0 else 0)
                cell_maxx = (
                    minx
                    + ((i + 1) * x_step)
                    + (self.overlap_buffer if i < grid_size - 1 else 0)
                )
                cell_miny = miny + (j * y_step) - (self.overlap_buffer if j > 0 else 0)
                cell_maxy = (
                    miny
                    + ((j + 1) * y_step)
                    + (self.overlap_buffer if j < grid_size - 1 else 0)
                )

                chunk_bounds.append((cell_minx, cell_miny, cell_maxx, cell_maxy))

        return chunk_bounds

    def _extract_chunk(
        self, gdf: gpd.GeoDataFrame, bounds: Tuple[float, float, float, float]
    ) -> gpd.GeoDataFrame:
        """
        Extract features that fall within the specified bounds.

        Args:
            gdf: Full GeoDataFrame
            bounds: Chunk bounds (minx, miny, maxx, maxy)

        Returns:
            GeoDataFrame with features in the chunk
        """
        # Create a boundary polygon
        boundary = box(*bounds)

        # Use spatial index if available
        if hasattr(gdf, "sindex") and gdf.sindex is not None:
            # Query the spatial index
            possible_matches_idx = list(gdf.sindex.intersection(bounds))
            if possible_matches_idx:
                possible_matches = gdf.iloc[possible_matches_idx]
                return possible_matches[possible_matches.intersects(boundary)].copy()
            return gpd.GeoDataFrame(crs=gdf.crs)
        else:
            # Fallback for when sindex isn't available
            return gdf[gdf.intersects(boundary)].copy()


def process_layers_in_chunks(
    all_layers: Dict[str, gpd.GeoDataFrame],
    chunk_size: int = 5000,
    overlap_buffer: float = 50.0,
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Process all layers in spatial chunks to reduce memory usage.

    Args:
        all_layers: Dictionary of layer GeoDataFrames
        chunk_size: Maximum features per chunk
        overlap_buffer: Buffer distance in meters for chunk overlap

    Returns:
        Dictionary of processed layer GeoDataFrames with connections
    """
    processor = ChunkedProcessor(chunk_size=chunk_size, overlap_buffer=overlap_buffer)
    processed_layers = {}

    for layer_key in all_layers:
        try:
            processed_layers[layer_key] = processor.process_layer_in_chunks(
                layer_key, all_layers
            )
        except Exception as e:
            logging.error(f"Error processing layer {layer_key} in chunks: {e}")
            # Use the original layer if processing fails
            processed_layers[layer_key] = all_layers[layer_key]

    return processed_layers


# Integration function for the main pipeline
def compute_connections_in_chunks(
    layers: Dict[str, gpd.GeoDataFrame],
    chunk_size: int = 5000,
    overlap_buffer: float = 50.0,
) -> Dict[str, gpd.GeoDataFrame]:
    """
    Drop-in replacement for compute_connections that processes in chunks.

    Args:
        layers: Dictionary of GeoDataFrames for each layer
        chunk_size: Maximum features per chunk
        overlap_buffer: Buffer distance for chunk overlap

    Returns:
        Updated dictionary with layers containing connection information
    """
    if not layers:
        logging.warning("No layers to process for connections")
        return {}

    # Ensure all layers have the right projection and source_layer
    reprojected_layers = {}
    for layer_key, gdf in layers.items():
        if "source_layer" not in gdf.columns:
            gdf["source_layer"] = layer_key

        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")

        reprojected_layers[layer_key] = gdf

    # Process all layers in chunks
    return process_layers_in_chunks(
        reprojected_layers, chunk_size=chunk_size, overlap_buffer=overlap_buffer
    )
