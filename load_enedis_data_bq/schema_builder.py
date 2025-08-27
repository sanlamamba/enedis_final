from typing import List, Dict, Any
from datetime import datetime
from google.cloud import bigquery


def infer_field_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        if len(value) == 10:
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return "DATE"
            except ValueError:
                pass
        return "STRING"
    return "STRING"


def build_schema(sample_rows: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    schema_dict = {}

    for row in sample_rows:
        for key, value in row.items():
            if key not in schema_dict and value is not None:
                if key == "geography":
                    schema_dict[key] = "GEOGRAPHY"
                else:
                    schema_dict[key] = infer_field_type(value)

    return [
        bigquery.SchemaField(name, field_type, mode="NULLABLE")
        for name, field_type in schema_dict.items()
    ]


def get_core_schema() -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("geography", "GEOGRAPHY", mode="NULLABLE"),
        bigquery.SchemaField("geometry_wkt", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geometry_geojson", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geometry_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("source_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="NULLABLE"),
    ]
