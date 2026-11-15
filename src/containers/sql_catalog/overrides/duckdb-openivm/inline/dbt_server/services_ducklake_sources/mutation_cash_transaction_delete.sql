-- catalog-override: DuckDB's SQLGlot dialect writer wraps modulo divisors in NULLIF; preserve exact deterministic score buckets.
DELETE FROM {table_relation} WHERE abs(((coalesce(ct_ca_id, 0) % 1000000) * 2654435761 + {batch_offset}) % 1000000) >= {start} AND abs(((coalesce(ct_ca_id, 0) % 1000000) * 2654435761 + {batch_offset}) % 1000000) < {end};
