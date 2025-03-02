from shapely.geometry import Point, LineString
from shapely.ops import transform
from shapely.geometry import shape
from enum import Enum
import pyproj


class CRS(Enum):
    EPSG_4326 = "4326"  # WGS 84
    EPSG_2154 = "2154"  # RGF93 / Lambert-93
    EPSG_3857 = "3857"  # Pseudo-Mercators


def get_endpoints(geom):
    """
    If geom is a LineString, return its start and end points;
    otherwise, return (None, None).
    """
    if isinstance(geom, LineString) and len(geom.coords) >= 2:
        return Point(geom.coords[0]), Point(geom.coords[-1])
    return None, None


def get_dual_coordinates(geom_dict, transformer):
    """
    Given a geometry dictionary (as produced by __geo_interface__)
    and a transformer, return a dictionary with both EPSG:4326 and EPSG:2154 coordinates.
    """
    geom_obj = shape(geom_dict)
    coords_2154 = geom_dict["coordinates"]
    geom_4326 = transform(transformer.transform, geom_obj)
    coords_4326 = geom_4326.__geo_interface__["coordinates"]
    return {"EPSG:4326": coords_4326, "EPSG:2154": coords_2154}


def get_coordinates(geom_dict, transformer):
    """
    Given a geometry dictionary (as produced by __geo_interface__)
    and a transformer, return coordinates in EPSG:4326.
    """
    geom_obj = shape(geom_dict)
    geom_4326 = transform(transformer.transform, geom_obj)
    coords_4326 = geom_4326.__geo_interface__["coordinates"]
    return coords_4326


def convert_coordinates(geom_dict, from_crs: CRS = None, to_crs: CRS = None):
    """
    Convert geometry coordinates from one CRS to another.

    :param geom_dict: A geometry dictionary (from __geo_interface__)
    :param from_crs: The source CRS (as a CRS Enum, optional)
    :param to_crs: The target CRS (as a CRS Enum, optional)
    :return: Converted coordinates in the target CRS or all CRSs if no specific CRS is provided.
    """
    geom_obj = shape(geom_dict)

    if from_crs and to_crs:
        transformer = pyproj.Transformer.from_crs(
            f"EPSG:{from_crs.value}", f"EPSG:{to_crs.value}", always_xy=True
        )
        transformed_geom = transform(transformer.transform, geom_obj)
        return transformed_geom.__geo_interface__["coordinates"]

    all_results = {}
    for crs in CRS:
        transformer = pyproj.Transformer.from_crs(
            f"EPSG:{CRS.EPSG_4326.value}", f"EPSG:{crs.value}", always_xy=True
        )
        all_results[crs.name] = transform(
            transformer.transform, geom_obj
        ).__geo_interface__["coordinates"]

    return all_results
