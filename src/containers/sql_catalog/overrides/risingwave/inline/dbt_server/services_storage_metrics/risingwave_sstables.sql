-- catalog-override: RisingWave storage metrics read internal SSTable metadata from rw_catalog.
SELECT CAST(table_ids AS VARCHAR), COALESCE(SUM(file_size), 0), COALESCE(SUM(uncompressed_file_size), 0), COALESCE(SUM(total_key_count), 0), COUNT(*) FROM rw_catalog.rw_hummock_sstables GROUP BY table_ids ORDER BY SUM(file_size) DESC
