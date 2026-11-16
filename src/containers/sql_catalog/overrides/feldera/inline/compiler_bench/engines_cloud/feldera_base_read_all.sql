-- catalog-override: engine-only: Feldera reads the base query when schema metadata is unavailable for verification.
SELECT * FROM ({sql}) __cb
