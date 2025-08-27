import logging
import time
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from schema_builder import build_schema, get_core_schema

logger = logging.getLogger(__name__)


def ensure_dataset(
    bq_client: bigquery.Client, project_id: str, dataset_id: str, location: str
):
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        bq_client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        bq_client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset {dataset_ref}")


def create_or_update_table(
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_name: str,
    sample_rows: List[Dict[str, Any]],
) -> bigquery.Table:

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    dynamic_schema = build_schema(sample_rows)
    core_schema = get_core_schema()
    core_field_names = {field.name for field in core_schema}

    schema = core_schema + [
        field for field in dynamic_schema if field.name not in core_field_names
    ]

    try:
        table = bq_client.get_table(table_id)
        existing_fields = {field.name for field in table.schema}
        new_fields = [field for field in schema if field.name not in existing_fields]

        if new_fields:
            table.schema = table.schema + new_fields
            table = bq_client.update_table(table, ["schema"])
            logger.info(f"Updated table {table_id} with {len(new_fields)} new fields")

    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table)
        logger.info(f"Created table {table_id}")
        time.sleep(5)
        table = bq_client.get_table(table_id)

    return table


def insert_rows(
    bq_client: bigquery.Client,
    table: bigquery.Table,
    rows: List[Dict[str, Any]],
    max_retries: int = 3,
):
    if not rows:
        return

    import time

    for attempt in range(max_retries):
        try:
            bq_client.get_table(table)

            errors = bq_client.insert_rows_json(table, rows)

            if errors:
                if "not found" in str(errors).lower() and attempt < max_retries - 1:
                    logger.warning(
                        f"Table not found, retrying in 5 seconds... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(5)
                    table = bq_client.get_table(table.reference)
                    continue
                raise Exception(f"BigQuery insertion failed: {errors}")

            logger.info(f"Inserted {len(rows)} rows into {table.table_id}")
            return

        except NotFound as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Table disappeared, waiting 5 seconds... (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(5)
                try:
                    table = bq_client.get_table(table.reference)
                except:
                    raise e
            else:
                raise e

    raise Exception(f"Failed to insert after {max_retries} attempts")
