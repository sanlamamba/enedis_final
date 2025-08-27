import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Iterator
from geometry_converter import geojson_to_wkt, extract_point_coords


def clean_property_key(key: str) -> str:
    return key.replace("-", "_").replace(" ", "_").lower()


def process_property_value(value: Any, key: str) -> Any:
    if key == "code_commune":
        if value is not None:
            try:
                return str(int(float(value)))
            except (ValueError, TypeError):
                return str(value) if value else None
        return None

    if isinstance(value, list):
        if all(isinstance(item, (str, int, float)) for item in value):
            return ", ".join(str(item) for item in value)
        return json.dumps(value)

    return value


def process_feature(feature: Dict, date: str) -> Dict[str, Any]:
    row = {}

    properties = feature.get("properties", {})
    for key, value in properties.items():
        clean_key = clean_property_key(key)
        row[clean_key] = process_property_value(value, clean_key)

    geometry = feature.get("geometry", {})
    if geometry:
        wkt = geojson_to_wkt(geometry)
        if wkt:
            row["geography"] = wkt
            row["geometry_wkt"] = wkt

        row["geometry_geojson"] = json.dumps(geometry)
        row["geometry_type"] = geometry.get("type")

        longitude, latitude = extract_point_coords(geometry)
        if longitude is not None:
            row["longitude"] = longitude
            row["latitude"] = latitude

    row["source_date"] = date
    row["load_timestamp"] = datetime.now(timezone.utc).isoformat()

    return row


def process_features_batch(
    features: List[Dict], date: str, chunk_size: int
) -> Iterator[List[Dict[str, Any]]]:
    for i in range(0, len(features), chunk_size):
        chunk = features[i : i + chunk_size]
        yield [process_feature(feature, date) for feature in chunk]


def extract_date_from_path(blob_name: str) -> str:
    path_parts = blob_name.split("/")
    if len(path_parts) >= 2:
        potential_date = path_parts[1]
        try:
            datetime.strptime(potential_date, "%Y-%m-%d")
            return potential_date
        except ValueError:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_table_name_from_path(blob_name: str) -> str:
    filename = blob_name.split("/")[-1]
    return filename.replace(".geojson", "").replace("-", "_").lower()
