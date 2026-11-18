-- catalog-override: Materialize exposes compiled query output types through a zero-row query.
SELECT * FROM (
{query}
) AS expected LIMIT 0
