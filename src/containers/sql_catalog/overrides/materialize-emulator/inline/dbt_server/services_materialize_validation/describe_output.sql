-- catalog-override: Materialize exposes maintained output types through a zero-row query.
SELECT * FROM {relation} LIMIT 0
