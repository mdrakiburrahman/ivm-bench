-- catalog-override: Materialize represents the source TINYINT tier as SMALLINT; null arithmetic preserves the canonical null case.
UPDATE {table_relation}
SET tier = CAST(((CAST(tier AS INTEGER) + 1) % 100) AS SMALLINT)
WHERE abs(((coalesce(customerid, 0) % 1000000) * 2654435761 + {batch_offset}) % 1000000) >= {start}
  AND abs(((coalesce(customerid, 0) % 1000000) * 2654435761 + {batch_offset}) % 1000000) < {end};
