-- catalog-override: RisingWave copies a bounded connector snapshot into a mutable table.
CREATE TABLE {relation} AS SELECT {projection} FROM {source_relation} FOR SYSTEM_VERSION AS OF {snapshot}
