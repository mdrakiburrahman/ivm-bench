-- catalog-override: Materialize compares maintained outputs with compiled definitions using EXCEPT ALL.
WITH expected AS (
{query}
)
(SELECT * FROM {relation} EXCEPT ALL SELECT * FROM expected)
UNION ALL
(SELECT * FROM expected EXCEPT ALL SELECT * FROM {relation})
LIMIT 1
