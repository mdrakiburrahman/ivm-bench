-- catalog-override: RisingWave CREATE SOURCE connector DDL is unsupported by the canonical Spark SQL dialect; preserve the native source syntax.
CREATE SOURCE {relation} ({columns}) WITH ({properties}) FORMAT PLAIN ENCODE PARQUET
