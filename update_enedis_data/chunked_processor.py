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

        # Create a spatial index for faster feature lookup
        if not hasattr(full_gdf_proj, "sindex") or full_gdf_proj.sindex is None:
            full_gdf_proj = full_gdf_proj.copy()  # Ensure we can modify it
            full_gdf_proj.sindex  # This creates the spatial index

        processed_chunks = []
        all_connections = {}  # Track all connections for later deduplication

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

            # Add source_layer if not present
            if "source_layer" not in chunk_gdf.columns:
                chunk_gdf["source_layer"] = layer_key

            try:
                # Create optimized projected layers dictionary with only needed data
                all_layers_proj = {}
                for k, gdf in all_layers.items():
                    if gdf.crs != "EPSG:3857":
                        # Include only geometry and id for spatial lookup to save memory
                        min_gdf = gdf[["id", "geometry"]].copy()
                        min_gdf["source_layer"] = k
                        all_layers_proj[k] = min_gdf.to_crs(epsg=3857)
                    else:
                        min_gdf = gdf[["id", "geometry"]].copy()
                        min_gdf["source_layer"] = k
                        all_layers_proj[k] = min_gdf

                # Create a combined GeoDataFrame with relevant features for this chunk
                # Instead of using the entire dataset, create a spatial filter
                chunk_buffer = box(*bounds).buffer(self.overlap_buffer * 2)
                relevant_features = []

                for k, gdf in all_layers_proj.items():
                    if hasattr(gdf, "sindex") and gdf.sindex is not None:
                        # Use spatial index for efficient filtering
                        possible_matches_idx = list(
                            gdf.sindex.intersection(chunk_buffer.bounds)
                        )
                        if possible_matches_idx:
                            matches = gdf.iloc[possible_matches_idx]
                            matches = matches[matches.intersects(chunk_buffer)]
                            relevant_features.append(matches)
                    else:
                        # Fallback if no spatial index
                        matches = gdf[gdf.intersects(chunk_buffer)]
                        relevant_features.append(matches)

                if not relevant_features:
                    logging.warning(f"No relevant features found for chunk {i+1}")
                    continue

                # Combine relevant features from all layers
                all_features_combined = pd.concat(relevant_features, ignore_index=True)
                all_features_combined = gpd.GeoDataFrame(
                    all_features_combined, geometry="geometry", crs="EPSG:3857"
                )

                # Ensure we have spatial index for the combined features
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

                # Store connections for later deduplication
                for _, row in chunk_with_connections.iterrows():
                    feat_id = row["id"]
                    connections = row.get("connections", [])
                    start_conn = row.get("start_connections", [])
                    end_conn = row.get("end_connections", [])

                    if feat_id in all_connections:
                        # Merge connections if feature was in multiple chunks
                        all_connections[feat_id]["connections"] = list(
                            set(all_connections[feat_id]["connections"] + connections)
                        )
                        all_connections[feat_id]["start_connections"] = list(
                            set(
                                all_connections[feat_id]["start_connections"]
                                + start_conn
                            )
                        )
                        all_connections[feat_id]["end_connections"] = list(
                            set(all_connections[feat_id]["end_connections"] + end_conn)
                        )
                    else:
                        all_connections[feat_id] = {
                            "connections": connections,
                            "start_connections": start_conn,
                            "end_connections": end_conn,
                        }

                # Convert back to WGS84
                chunk_with_connections = chunk_with_connections.to_crs(epsg=4326)

                # Add to processed chunks
                processed_chunks.append(chunk_with_connections)
                logging.info(
                    f"Processed chunk {i+1} with {len(chunk_with_connections)} features"
                )

            except Exception as e:
                logging.error(f"Error processing chunk {i+1}: {e}")
                # Continue with other chunks instead of failing entirely
                continue

        if not processed_chunks:
            logging.warning(
                f"No chunks were successfully processed for layer {layer_key}"
            )
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

        # Combine all processed chunks
        combined_gdf = pd.concat(processed_chunks, ignore_index=True)

        # Remove duplicates keeping first occurrence
        combined_gdf = combined_gdf.drop_duplicates(subset=["id"])

        # Update connections with the merged connections from all chunks
        for i, row in combined_gdf.iterrows():
            feat_id = row["id"]
            if feat_id in all_connections:
                combined_gdf.at[i, "connections"] = all_connections[feat_id][
                    "connections"
                ]
                combined_gdf.at[i, "start_connections"] = all_connections[feat_id][
                    "start_connections"
                ]
                combined_gdf.at[i, "end_connections"] = all_connections[feat_id][
                    "end_connections"
                ]

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

        # Determine number of chunks based on data size and spatial distribution
        num_chunks = max(1, int(np.ceil(len(gdf) / self.chunk_size)))

        # Calculate grid dimensions - aim for square chunks
        grid_size = max(1, int(np.ceil(np.sqrt(num_chunks))))

        # Adjust grid for non-square areas to minimize overlap and maximize efficiency
        aspect_ratio = (maxx - minx) / (maxy - miny)
        if aspect_ratio > 1.5:
            # Area is wider than tall - use more columns
            cols = int(grid_size * aspect_ratio / 1.5)
            rows = int(np.ceil(num_chunks / cols))
        elif aspect_ratio < 0.67:
            # Area is taller than wide - use more rows
            rows = int(grid_size / aspect_ratio * 0.67)
            cols = int(np.ceil(num_chunks / rows))
        else:
            # Roughly square area
            rows = grid_size
            cols = grid_size

        # Calculate cell sizes
        x_step = (maxx - minx) / cols
        y_step = (maxy - miny) / rows

        # Generate grid cells with overlap
        chunk_bounds = []
        for i in range(cols):
            for j in range(rows):
                cell_minx = minx + (i * x_step) - (self.overlap_buffer if i > 0 else 0)
                cell_maxx = (
                    minx
                    + ((i + 1) * x_step)
                    + (self.overlap_buffer if i < cols - 1 else 0)
                )
                cell_miny = miny + (j * y_step) - (self.overlap_buffer if j > 0 else 0)
                cell_maxy = (
                    miny
                    + ((j + 1) * y_step)
                    + (self.overlap_buffer if j < rows - 1 else 0)
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
                # We're careful not to modify the original GDF
                return possible_matches[possible_matches.intersects(boundary)].copy()
            return gpd.GeoDataFrame(crs=gdf.crs)
        else:
            # If no spatial index available, build one first for better performance
            try:
                # This creates a spatial index
                gdf.sindex
                return self._extract_chunk(gdf, bounds)
            except Exception as e:
                logging.warning(
                    f"Failed to create spatial index: {e}, using slower method"
                )
                # Fallback - slower but works without spatial index
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

    # Define processing order based on layer dependencies
    processing_order = []

    # Process point layers first (they're typically used as connection anchors)
    for layer_key in all_layers:
        if all_layers[layer_key].iloc[0].geometry.geom_type == "Point":
            processing_order.append(layer_key)

    # Then process line layers
    for layer_key in all_layers:
        if layer_key not in processing_order:
            processing_order.append(layer_key)

    for layer_key in processing_order:
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
        if gdf.empty:
            logging.warning(f"Layer {layer_key} has no features - skipping")
            continue

        if "source_layer" not in gdf.columns:
            gdf["source_layer"] = layer_key

        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")

        # Validate essential columns
        if "id" not in gdf.columns:
            logging.error(f"Layer {layer_key} missing required 'id' column")
            continue

        # Check for invalid geometries and fix them if possible
        invalid_geoms = ~gdf.geometry.is_valid
        if invalid_geoms.any():
            invalid_count = invalid_geoms.sum()
            logging.warning(f"Found {invalid_count} invalid geometries in {layer_key}")

            # Try to fix invalid geometries
            gdf = gdf.copy()
            for idx in gdf[invalid_geoms].index:
                try:
                    gdf.loc[idx, "geometry"] = gdf.loc[idx, "geometry"].buffer(0)
                except Exception:
                    logging.error(f"Failed to fix invalid geometry at index {idx}")

        reprojected_layers[layer_key] = gdf

    # Validate if we have any valid layers to process
    if not reprojected_layers:
        logging.error("No valid layers to process after validation")
        return {}

    # Process all layers in chunks
    return process_layers_in_chunks(
        reprojected_layers, chunk_size=chunk_size, overlap_buffer=overlap_buffer
    )
