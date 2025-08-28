from google.cloud import bigquery
import pandas as pd
from typing import Optional


class BigQueryClient:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.client.query(query).to_dataframe()

    def get_single_value(self, query: str, column: str):
        df = self.execute_query(query)
        return df[column].iloc[0] if not df.empty else None

    def save_dataframe(
        self,
        df: pd.DataFrame,
        table_id: str,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    ):
        if df.empty:
            return

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition, autodetect=True
        )

        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    def save_batch(self, df: pd.DataFrame, table_id: str, is_first_batch: bool = False):
        disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if is_first_batch
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        self.save_dataframe(df, table_id, disposition)
