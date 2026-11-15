-- catalog-override: RisingWave appends rows from a registered connector source into staging.
INSERT INTO {target_relation} ({columns}) SELECT {columns} FROM {source_relation}
