-- catalog-override: RisingWave ingestion counts staged rows over the engine's pgwire connection.
SELECT COUNT(*) FROM {relation}
