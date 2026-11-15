-- catalog-override: engine-only: SQLite dbstat reports pages consumed by inlined DuckLake data.
SELECT name, SUM(pgsize) FROM dbstat GROUP BY name
