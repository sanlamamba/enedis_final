from shapely.geometry import Point, LineString, shape
from shapely.ops import transform
from enum import Enum
import pyproj


class CRS(Enum):
    """
    Enumeration of common Coordinate Reference Systems.
    """

    EPSG_4326 = "4326"  # WGS 84
    EPSG_2154 = "2154"  # RGF93 / Lambert-93
    EPSG_3857 = "3857"  # Pseudo-Mercators


def get_endpoints(geom):
    """
    Return the start and end points of a LineString geometry.

    Parameters:
        geom: A Shapely geometry.

    Returns:
        tuple: (start_point, end_point) if geom is a valid LineString; otherwise, (None, None).
    """
    if isinstance(geom, LineString) and len(geom.coords) >= 2:
        return Point(geom.coords[0]), Point(geom.coords[-1])
    return None, None


def get_dual_coordinates(geom_dict, transformer):
    """
    Return dual coordinate representations for a geometry.

    Parameters:
        geom_dict (dict): A geometry dictionary (from __geo_interface__).
        transformer: A pyproj Transformer to convert coordinates to EPSG:4326.

    Returns:
        dict: A dictionary with keys "EPSG:4326" (transformed) and "EPSG:2154" (original).
    """
    geom_obj = shape(geom_dict)
    coords_2154 = geom_dict["coordinates"]
    geom_4326 = transform(transformer.transform, geom_obj)
    coords_4326 = geom_4326.__geo_interface__["coordinates"]
    return {"EPSG:4326": coords_4326, "EPSG:2154": coords_2154}


def get_coordinates(geom_dict, transformer):
    """
    Transform a geometry's coordinates to EPSG:4326.

    Parameters:
        geom_dict (dict): A geometry dictionary.
        transformer: A pyproj Transformer for converting coordinates.

    Returns:
        The geometry's coordinates in EPSG:4326.
    """
    geom_obj = shape(geom_dict)
    geom_4326 = transform(transformer.transform, geom_obj)
    return geom_4326.__geo_interface__["coordinates"]


def convert_coordinates(geom_dict, from_crs: CRS = None, to_crs: CRS = None):
    """
    Convert geometry coordinates from one CRS to another.

    If both 'from_crs' and 'to_crs' are provided, return the coordinates in the target CRS.
    Otherwise, return a dictionary with coordinates converted from EPSG:4326 to all available CRSs.

    Parameters:
        geom_dict (dict): A geometry dictionary (from __geo_interface__).
        from_crs (CRS, optional): The source CRS.
        to_crs (CRS, optional): The target CRS.

    Returns:
        The converted coordinates in the target CRS, or a dictionary of conversions.
    """
    geom_obj = shape(geom_dict)

    if from_crs and to_crs:
        transformer = pyproj.Transformer.from_crs(
            f"EPSG:{from_crs.value}", f"EPSG:{to_crs.value}", always_xy=True
        )
        transformed_geom = transform(transformer.transform, geom_obj)
        return transformed_geom.__geo_interface__["coordinates"]

    converted_coords = {}
    for crs in CRS:
        transformer = pyproj.Transformer.from_crs(
            f"EPSG:{CRS.EPSG_4326.value}", f"EPSG:{crs.value}", always_xy=True
        )
        converted_coords[crs.name] = transform(
            transformer.transform, geom_obj
        ).__geo_interface__["coordinates"]

    return converted_coords
