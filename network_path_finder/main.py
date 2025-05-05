#!/usr/bin/env python3
"""
Script to find the closest BT network element to a geographic point and
trace connections to find the closest source substation.
"""

import logging
from finder import find_closest_path
from config import MAX_BT_DISTANCE

if __name__ == "__main__":

    TARGET_LONGITUDE = 2.447829364048289
    TARGET_LATITUDE = 48.861599840010655

    logging.info(
        f"Starting network path finder with maximum distance of {MAX_BT_DISTANCE} km"
    )
    result = find_closest_path(
        lon=TARGET_LONGITUDE,
        lat=TARGET_LATITUDE,
    )

    if result and result.get("success", False):
        logging.info("Successfully found path to source substation")
    else:
        logging.error(
            f"Failed to find path to source substation within {MAX_BT_DISTANCE} km"
        )
