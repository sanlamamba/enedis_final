"""
Utility functions for the Enedis project.
This module contains shared utility functions used across the application.
"""

import os
import json
import logging
import tempfile
import functools
import time
from datetime import datetime
from typing import Any, Dict, Optional, Callable, TypeVar, List

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

T = TypeVar("T")


def timed(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to measure and log function execution time.

    Args:
        func: The function to time

    Returns:
        The wrapped function with timing
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logging.info(f"Function {func.__name__} executed in {elapsed_time:.3f} seconds")
        return result

    return wrapper


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    logger: Optional[logging.Logger] = None,
):
    """
    Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch and retry
        logger: Logger to use, defaults to root logger if None

    Returns:
        The decorated function
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = logger or logging.getLogger()
            attempts = 0
            current_delay = delay

            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    if attempts == max_attempts:
                        log.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    log.warning(
                        f"Attempt {attempts} failed for {func.__name__}: {e}. Retrying in {current_delay:.2f}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

            return None  # Should not reach here

        return wrapper

    return decorator


def extract_geometry_from_json(geojson_str: str) -> Optional[Any]:
    """
    Safely extract a Shapely geometry from a GeoJSON string.

    Args:
        geojson_str: GeoJSON string representation

    Returns:
        A shapely geometry object or None if extraction fails
    """
    try:
        geo_dict = json.loads(geojson_str)
        return shape(geo_dict)
    except Exception as e:
        logging.debug(f"Failed to parse geometry: {e}")
        return None


def add_filedate(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add a filedate column with today's date in YYYYMMDD format.

    Args:
        gdf: GeoDataFrame to add filedate to

    Returns:
        GeoDataFrame with filedate column added
    """
    gdf["filedate"] = datetime.today().strftime("%Y%m%d")
    return gdf


def create_temp_file(suffix: str = ".tmp") -> str:
    """
    Create a temporary file and return its path.
    The caller is responsible for deleting the file when done.

    Args:
        suffix: File suffix/extension

    Returns:
        Path to the temporary file
    """
    temp_file = tempfile.NamedTemporaryFile("w+", delete=False, suffix=suffix)
    temp_file.close()
    return temp_file.name


def clean_layer_dataframe(gdf: gpd.GeoDataFrame, layer_key: str) -> gpd.GeoDataFrame:
    """
    Clean and prepare a layer GeoDataFrame by adding source_layer and ensuring CRS.

    Args:
        gdf: GeoDataFrame to clean
        layer_key: Key name for the layer

    Returns:
        Cleaned GeoDataFrame
    """
    if "source_layer" not in gdf.columns:
        gdf["source_layer"] = layer_key

    if gdf.crs is None:
        gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)

    return gdf


def merge_geodataframes(gdfs: List[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """
    Merge multiple GeoDataFrames into one.

    Args:
        gdfs: List of GeoDataFrames to merge

    Returns:
        Merged GeoDataFrame
    """
    if not gdfs:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

    if merged.crs is None:
        merged.set_crs(gdfs[0].crs or "EPSG:4326", inplace=True)

    return merged
