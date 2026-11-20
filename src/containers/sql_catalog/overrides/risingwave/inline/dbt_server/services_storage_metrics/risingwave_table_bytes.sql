-- catalog-override: RisingWave storage metrics read internal table statistics from rw_catalog.
SELECT c.id, c.name, COALESCE(st.total_key_size, 0) + COALESCE(st.total_value_size, 0) AS bytes, COALESCE(st.total_key_count, 0) AS rows FROM rw_catalog.{view} c JOIN rw_catalog.rw_table_stats st ON st.id = c.id
