-- catalog-override: RisingWave compiler probes infer Parquet columns through read_parquet with LIMIT 0.
SELECT * FROM read_parquet({path}) LIMIT 0
