import pandas as pd
import time
from typing import Dict, Optional
from functools import partial

from config import Config
from bigquery_client import BigQueryClient
from queries import (
    build_site_count_query,
    build_batch_query,
    build_connections_query,
    build_finalize_query,
)
from pathfinding import collect_elements_to_query, find_path_bfs
from data_processing import (
    process_connections_batch,
    create_site_result,
    process_batch_dataframe,
)


class GridProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.client = BigQueryClient(config.project_id)
        self.connections_cache: Dict = {}

    def get_total_sites(self) -> int:
        query = build_site_count_query(self.config)
        return self.client.get_single_value(query, "total_sites")

    def fetch_connections_batch(self, element_ids: list) -> Dict:
        if not element_ids:
            return {}

        query = build_connections_query(self.config, element_ids)
        result = self.client.execute_query(query)
        return process_connections_batch(result)

    def find_path_to_poste(self, start_id: str) -> tuple:
        elements_needed = collect_elements_to_query(
            start_id, self.connections_cache, self.config.exploration_limit
        )

        new_elements = list(elements_needed - set(self.connections_cache.keys()))
        if new_elements:
            batch_connections = self.fetch_connections_batch(new_elements)
            self.connections_cache.update(batch_connections)

        return find_path_bfs(
            start_id,
            self.connections_cache,
            self.config.poste_source_layer,
            self.config.max_depth,
        )

    def process_site(self, row: pd.Series) -> Dict:
        entry_point = row.get("closest_bt_id")

        if pd.notna(entry_point) and entry_point:
            poste_source, path = self.find_path_to_poste(entry_point)
            return create_site_result(row, poste_source, path)

        return create_site_result(row)

    def process_batch(self, offset: int, limit: int) -> pd.DataFrame:
        query = build_batch_query(self.config, offset, limit)
        batch_df = self.client.execute_query(query)

        return process_batch_dataframe(batch_df, self.process_site)

    def run(self):
        total_sites = self.get_total_sites()
        print(
            f"Processing {total_sites:,} sites in batches of {self.config.batch_size}"
        )

        processed = 0
        batch_num = 0

        while processed < total_sites:
            current_batch_size = min(self.config.batch_size, total_sites - processed)
            print(
                f"Batch {batch_num + 1}: sites {processed + 1:,}-{processed + current_batch_size:,}"
            )

            try:
                batch_results = self.process_batch(processed, current_batch_size)

                if not batch_results.empty:
                    self.client.save_batch(
                        batch_results,
                        self.config.temp_table,
                        is_first_batch=(batch_num == 0),
                    )
                    print(f"Saved {len(batch_results)} results")

            except Exception as e:
                print(f"Error in batch {batch_num + 1}: {e}")

            processed += current_batch_size
            batch_num += 1

            if batch_num % 10 == 0:
                progress = (processed / total_sites) * 100
                print(f"Progress: {progress:.1f}%")

            time.sleep(0.5)

        self.finalize()

    def finalize(self):
        query = build_finalize_query(self.config.temp_table, self.config.final_table)
        self.client.execute_query(query)
        print(f"Results saved to {self.config.final_table}")
