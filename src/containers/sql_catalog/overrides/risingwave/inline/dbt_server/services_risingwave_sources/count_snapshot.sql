-- catalog-override: RisingWave counts the explicitly pinned Iceberg snapshot before copying it.
SELECT COUNT(*) FROM {relation} FOR SYSTEM_VERSION AS OF {snapshot}
