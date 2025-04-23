#!/usr/bin/env python3
"""
Script to find the closest BT network element to a geographic point and
trace connections to find the closest source substation.
"""

from .finder import find_closest_path

if __name__ == "__main__":
    TARGET_LONGITUDE = 2.3522
    TARGET_LATITUDE = 48.8566

    find_closest_path(
        lon=TARGET_LONGITUDE,
        lat=TARGET_LATITUDE,
    )
