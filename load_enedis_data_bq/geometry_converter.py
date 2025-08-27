from typing import Dict, Optional, List, Tuple


def coords_to_wkt_points(coords: List[Tuple[float, float]]) -> str:
    return ", ".join([f"{lon} {lat}" for lon, lat in coords])


def ring_to_wkt(ring: List[Tuple[float, float]]) -> str:
    return f"({coords_to_wkt_points(ring)})"


def geojson_to_wkt(geometry: Dict) -> Optional[str]:
    if not geometry or not geometry.get("coordinates"):
        return None

    geom_type = geometry.get("type", "").upper()
    coords = geometry.get("coordinates", [])

    converters = {
        "POINT": lambda c: f"POINT({c[0]} {c[1]})" if len(c) >= 2 else None,
        "LINESTRING": lambda c: f"LINESTRING({coords_to_wkt_points(c)})",
        "POLYGON": lambda c: f"POLYGON({', '.join(map(ring_to_wkt, c))})",
        "MULTIPOINT": lambda c: f"MULTIPOINT({', '.join([f'({lon} {lat})' for lon, lat in c])})",
        "MULTILINESTRING": lambda c: f"MULTILINESTRING({', '.join(map(ring_to_wkt, c))})",
        "MULTIPOLYGON": lambda c: f"MULTIPOLYGON({', '.join([f'({', '.join(map(ring_to_wkt, polygon))})' for polygon in c])})",
    }

    try:
        converter = converters.get(geom_type)
        return converter(coords) if converter else None
    except (TypeError, IndexError, ValueError):
        return None


def extract_point_coords(geometry: Dict) -> Tuple[Optional[float], Optional[float]]:
    if geometry.get("type") == "Point" and geometry.get("coordinates"):
        coords = geometry.get("coordinates")
        if len(coords) >= 2:
            return coords[0], coords[1]
    return None, None
