-- catalog-override: Materialize exposes maintained outputs through its system catalog.
SELECT s.name, m.name
FROM mz_catalog.mz_materialized_views AS m
JOIN mz_catalog.mz_schemas AS s ON m.schema_id = s.id
JOIN mz_catalog.mz_databases AS d ON s.database_id = d.id
WHERE d.name = current_database()
