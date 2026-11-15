-- catalog-override: RisingWave compiler validation wraps the query in an engine-specific subquery probe.
SELECT * FROM ({query}) __cb LIMIT 0
