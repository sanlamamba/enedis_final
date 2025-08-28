def build_site_count_query(config):
    return f"""
    SELECT COUNT(*) as total_sites 
    FROM `{config.sites_table}`
    """


def build_batch_query(config, offset, limit):
    bt_layers = ", ".join([f"'{layer}'" for layer in config.bt_layers])

    return f"""
    WITH sites_batch AS (
        SELECT * FROM `{config.sites_table}`
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
        CROSS JOIN `{config.enedis_table}` e
        WHERE e.layer = '{config.poste_source_layer}'
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
        CROSS JOIN `{config.enedis_table}` e
        WHERE e.layer IN ({bt_layers})
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


def build_connections_query(config, element_ids):
    ids_str = "', '".join(element_ids)
    return f"""
    SELECT id, connections, layer
    FROM `{config.enedis_table}`
    WHERE id IN ('{ids_str}')
    """


def build_finalize_query(temp_table, final_table):
    return f"""
    CREATE OR REPLACE TABLE `{final_table}` AS
    SELECT *
    FROM `{temp_table}`
    ORDER BY id
    """
