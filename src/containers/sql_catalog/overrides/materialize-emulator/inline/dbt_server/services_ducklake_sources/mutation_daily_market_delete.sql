-- catalog-override: Materialize computes date offsets using date subtraction rather than Spark date_diff.
DELETE FROM {table_relation}
WHERE abs(((dm_date - DATE '1970-01-01') * 2654435761 + coalesce(ascii(substr(dm_s_symb, 1, 1)), 0) * 9176 + coalesce(ascii(substr(dm_s_symb, 2, 1)), 0) + {batch_offset}) % 1000000) >= {start}
  AND abs(((dm_date - DATE '1970-01-01') * 2654435761 + coalesce(ascii(substr(dm_s_symb, 1, 1)), 0) * 9176 + coalesce(ascii(substr(dm_s_symb, 2, 1)), 0) + {batch_offset}) % 1000000) < {end};
