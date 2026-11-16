-- catalog-override: engine-only: Feldera compares row bags using a grouped count, total rows, and multiplicity-weighted checksum.
SELECT count(*) AS groups, sum(__n) AS rows_total, sum(__n * __h) AS checksum FROM (SELECT count(*) AS __n, {row_hash} AS __h FROM {relation} GROUP BY {columns}) __g
