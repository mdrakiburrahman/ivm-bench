-- catalog-override: Materialize maps persistent shards to objects through its system catalog.
SELECT o.id, o.name, o.type, s.name, sh.shard_id
FROM mz_catalog.mz_objects AS o
JOIN mz_internal.mz_storage_shards AS sh ON sh.object_id = o.id
LEFT JOIN mz_catalog.mz_schemas AS s ON o.schema_id = s.id
