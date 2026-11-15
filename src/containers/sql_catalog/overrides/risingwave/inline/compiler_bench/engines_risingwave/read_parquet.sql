-- catalog-override: RisingWave compiler probes register Parquet input through the read_parquet table function.
SELECT * FROM read_parquet({path})
