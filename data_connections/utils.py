from shapely.geometry import Point, LineString
from shapely.ops import transform
from shapely.geometry import shape


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
