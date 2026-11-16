-- catalog-override: engine-only: Feldera normalizes nullable row values to strings for its bag digest.
coalesce(cast({value} AS VARCHAR), '\x00NULL')
