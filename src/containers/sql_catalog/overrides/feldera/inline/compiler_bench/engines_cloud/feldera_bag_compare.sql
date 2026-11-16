-- catalog-override: engine-only: Feldera compares grouped row multiplicities without EXCEPT on Z-set views.
SELECT count(*) AS diff FROM (SELECT {columns}, count(*) AS __n FROM {mv_name} GROUP BY {columns}) v FULL JOIN (SELECT {columns}, count(*) AS __n FROM ({sql}) __cb GROUP BY {columns}) q ON {predicates} WHERE v.__n IS DISTINCT FROM q.__n
