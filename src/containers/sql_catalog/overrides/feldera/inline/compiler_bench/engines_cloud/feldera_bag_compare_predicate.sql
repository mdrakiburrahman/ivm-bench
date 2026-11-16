-- catalog-override: engine-only: Feldera compares grouped output columns with NULL-safe equality.
(v.{column} IS NOT DISTINCT FROM q.{column})
