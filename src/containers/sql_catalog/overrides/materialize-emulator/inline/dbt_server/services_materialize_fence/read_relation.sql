-- catalog-override: Materialize needs a strict-serializable read of each maintained output.
SELECT 1 FROM {relation} LIMIT 1
