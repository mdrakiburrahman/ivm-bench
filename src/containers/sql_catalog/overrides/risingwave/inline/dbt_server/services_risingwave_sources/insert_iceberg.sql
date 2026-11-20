-- catalog-override: RisingWave appends a bounded Iceberg snapshot into its mutable staging table.
INSERT INTO {target_relation} ({columns}) SELECT {projection} FROM {source_relation} FOR SYSTEM_VERSION AS OF {snapshot}
