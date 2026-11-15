-- catalog-override: DuckDB's SQLGlot dialect writer wraps modulo divisors in NULLIF; preserve exact deterministic score buckets.
UPDATE {table_relation} SET accountdesc = accountdesc || ' upd' WHERE abs(((coalesce(accountid, 0) % 1000000) * 2654435761 + {batch_offset}) % 1000000) >= {start} AND abs(((coalesce(accountid, 0) % 1000000) * 2654435761 + {batch_offset}) % 1000000) < {end};
