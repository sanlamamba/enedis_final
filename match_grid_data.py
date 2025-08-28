import pandas as pd
from google.cloud import bigquery
import numpy as np
from collections import deque
import json
import time
import os


class OptimizedElectricalGridPathfinder:
    def __init__(self, batch_size=100):  # Reduced batch size
        self.client = bigquery.Client(project="ofr-2kt-valo-reseau-1-lab-prd")
        self.batch_size = batch_size
        self.temp_table = (
            "ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.temp_site_analysis"
        )
        self.connections_cache = {}

    def get_site_count(self):
        query = """
        SELECT COUNT(*) as total_sites 
        FROM `ofr-bdf-stor-reseau-1-prd.bdf_angele_prd.raw_dim_reh_site_t`
        """
        return self.client.query(query).to_dataframe()["total_sites"].iloc[0]

    def process_sites_batch_optimized(self, offset, limit):
        # Single query to process entire batch at once
        batch_query = f"""
        WITH sites_batch AS (
            SELECT * FROM `ofr-bdf-stor-reseau-1-prd.bdf_angele_prd.raw_dim_reh_site_t`
            WHERE gpsx IS NOT NULL AND gpsy IS NOT NULL 
            AND SAFE_CAST(gpsx AS FLOAT64) IS NOT NULL 
            AND SAFE_CAST(gpsy AS FLOAT64) IS NOT NULL
            ORDER BY id
            LIMIT {limit} OFFSET {offset}
        ),
        closest_postes_source AS (
            SELECT DISTINCT
                s.id as site_id,
                FIRST_VALUE(e.id) OVER (
                    PARTITION BY s.id 
                    ORDER BY ST_DISTANCE(
                        ST_GEOGPOINT(CAST(s.gpsx AS FLOAT64), CAST(s.gpsy AS FLOAT64)),
                        ST_GEOGPOINT(e.longitude, e.latitude)
                    )
                ) as closest_poste_source_id,
                FIRST_VALUE(ST_DISTANCE(
                    ST_GEOGPOINT(CAST(s.gpsx AS FLOAT64), CAST(s.gpsy AS FLOAT64)),
                    ST_GEOGPOINT(e.longitude, e.latitude)
                )) OVER (
                    PARTITION BY s.id 
                    ORDER BY ST_DISTANCE(
                        ST_GEOGPOINT(CAST(s.gpsx AS FLOAT64), CAST(s.gpsy AS FLOAT64)),
                        ST_GEOGPOINT(e.longitude, e.latitude)
                    )
                ) as poste_distance
            FROM sites_batch s
            CROSS JOIN `ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.enedis_full` e
            WHERE e.layer = 'postes_source'
        ),
        closest_bt_connections AS (
            SELECT DISTINCT
                s.id as site_id,
                FIRST_VALUE(e.id) OVER (
                    PARTITION BY s.id 
                    ORDER BY ST_DISTANCE(
                        ST_GEOGPOINT(CAST(s.gpsx AS FLOAT64), CAST(s.gpsy AS FLOAT64)),
                        ST_GEOGPOINT(e.longitude, e.latitude)
                    )
                ) as closest_bt_id,
                FIRST_VALUE(ST_DISTANCE(
                    ST_GEOGPOINT(CAST(s.gpsx AS FLOAT64), CAST(s.gpsy AS FLOAT64)),
                    ST_GEOGPOINT(e.longitude, e.latitude)
                )) OVER (
                    PARTITION BY s.id 
                    ORDER BY ST_DISTANCE(
                        ST_GEOGPOINT(CAST(s.gpsx AS FLOAT64), CAST(s.gpsy AS FLOAT64)),
                        ST_GEOGPOINT(e.longitude, e.latitude)
                    )
                ) as bt_distance
            FROM sites_batch s
            CROSS JOIN `ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.enedis_full` e
            WHERE e.layer IN ('reseau_bt', 'reseau_souterrain_bt')
        )
        SELECT 
            s.*,
            ps.closest_poste_source_id,
            ps.poste_distance,
            bt.closest_bt_id,
            bt.bt_distance
        FROM sites_batch s
        LEFT JOIN closest_postes_source ps ON s.id = ps.site_id
        LEFT JOIN closest_bt_connections bt ON s.id = bt.site_id
        ORDER BY s.id
        """

        print(f"Executing batch query for {limit} sites...")
        batch_df = self.client.query(batch_query).to_dataframe()

        if batch_df.empty:
            return pd.DataFrame()

        # Process path finding for each site
        results = []
        for _, row in batch_df.iterrows():
            result = self.process_single_site_with_precomputed(row)
            results.append(result)

        return pd.DataFrame(results)

    def process_single_site_with_precomputed(self, row):
        actual_poste_source = None
        path_elements = []
        path_length = 0

        entry_point_id = row.get("closest_bt_id")

        if pd.notna(entry_point_id) and entry_point_id:
            actual_poste_source, path_elements = self.find_path_to_poste_source_cached(
                entry_point_id
            )
            path_length = len(path_elements) if path_elements else 0

        return {
            "id": row["id"],
            "city": row["city"],
            "gpsx": row["gpsx"],
            "gpsy": row["gpsy"],
            "poste_source_proche": row.get("closest_poste_source_id"),
            "poste_source_proche_distance": row.get("poste_distance"),
            "entry_point_id": entry_point_id,
            "entry_point_distance": row.get("bt_distance"),
            "actual_poste_source": actual_poste_source,
            "path_elements": json.dumps(path_elements) if path_elements else None,
            "path_length": path_length,
        }

    def get_connections_batch(self, element_ids):
        """Get connections for multiple elements at once"""
        if not element_ids:
            return {}

        ids_str = "', '".join(element_ids)
        query = f"""
        SELECT id, connections, layer
        FROM `ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.enedis_full`
        WHERE id IN ('{ids_str}')
        """

        result = self.client.query(query).to_dataframe()

        connections_dict = {}
        for _, row in result.iterrows():
            element_id = row["id"]
            connections = row["connections"]
            layer = row["layer"]

            connected_ids = []
            if pd.notna(connections) and connections:
                connected_ids = [
                    conn.strip() for conn in str(connections).split(",") if conn.strip()
                ]

            connections_dict[element_id] = {
                "connections": connected_ids,
                "layer": layer,
            }

        return connections_dict

    def find_path_to_poste_source_cached(self, start_id):
        queue = deque([(start_id, [start_id])])
        visited = set()
        max_depth = 10  # Reduced max depth
        elements_to_query = set()

        # Collect all elements we need to query
        temp_queue = deque([start_id])
        temp_visited = set()

        while temp_queue and len(temp_visited) < 50:  # Limit exploration
            current_id = temp_queue.popleft()
            if current_id in temp_visited:
                continue
            temp_visited.add(current_id)
            elements_to_query.add(current_id)

            if current_id in self.connections_cache:
                for conn_id in self.connections_cache[current_id]["connections"]:
                    temp_queue.append(conn_id)

        # Get all needed connections at once
        new_elements = elements_to_query - set(self.connections_cache.keys())
        if new_elements:
            batch_connections = self.get_connections_batch(list(new_elements))
            self.connections_cache.update(batch_connections)

        # Now do the actual pathfinding
        while queue:
            current_id, path = queue.popleft()

            if len(path) > max_depth:
                continue

            if current_id in visited:
                continue
            visited.add(current_id)

            if current_id not in self.connections_cache:
                continue

            element_info = self.connections_cache[current_id]

            if element_info["layer"] == "postes_source":
                return current_id, path

            for connected_id in element_info["connections"]:
                if connected_id not in visited:
                    queue.append((connected_id, path + [connected_id]))

        return None, []

    def save_batch_results(self, results_df, is_first_batch=False):
        if results_df.empty:
            return

        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if is_first_batch
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition, autodetect=True
        )

        job = self.client.load_table_from_dataframe(
            results_df, self.temp_table, job_config=job_config
        )
        job.result()

    def process_all_sites(self):
        total_sites = self.get_site_count()
        print(f"Processing {total_sites:,} sites in batches of {self.batch_size}")

        processed = 0
        batch_num = 0

        while processed < total_sites:
            current_batch_size = min(self.batch_size, total_sites - processed)
            print(
                f"Processing batch {batch_num + 1}, sites {processed + 1:,}-{processed + current_batch_size:,}"
            )

            try:
                batch_results = self.process_sites_batch_optimized(
                    processed, current_batch_size
                )

                if not batch_results.empty:
                    self.save_batch_results(
                        batch_results, is_first_batch=(batch_num == 0)
                    )
                    print(f"Saved {len(batch_results)} results")
                else:
                    print("No results in this batch")

            except Exception as e:
                print(f"Error processing batch {batch_num + 1}: {e}")
                # Continue with next batch

            processed += current_batch_size
            batch_num += 1

            # Progress update every 10 batches
            if batch_num % 10 == 0:
                progress = (processed / total_sites) * 100
                print(f"Progress: {progress:.1f}% ({processed:,}/{total_sites:,})")

            time.sleep(0.5)  # Brief pause

        self.finalize_results()

    def finalize_results(self):
        final_table = "ofr-2kt-valo-reseau-1-lab-prd.ofr_2kt_enedis.site_grid_analysis"

        query = f"""
        CREATE OR REPLACE TABLE `{final_table}` AS
        SELECT *
        FROM `{self.temp_table}`
        ORDER BY id
        """

        self.client.query(query).result()
        print(f"Results saved to {final_table}")

        # Optional: cleanup temp table
        # cleanup_query = f"DROP TABLE `{self.temp_table}`"
        # self.client.query(cleanup_query).result()


def main():
    batch_size = int(os.getenv("BATCH_SIZE", "100"))  # Smaller default
    pathfinder = OptimizedElectricalGridPathfinder(batch_size=batch_size)

    print("Starting optimized electrical grid analysis...")
    pathfinder.process_all_sites()

    print("Analysis complete!")


if __name__ == "__main__":
    main()
