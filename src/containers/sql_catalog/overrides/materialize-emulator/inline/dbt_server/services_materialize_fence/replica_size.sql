-- catalog-override: Materialize exposes cluster replica resources through its system catalog.
SELECT c.name, r.size, s.processes, s.workers, s.cpu_nano_cores, s.memory_bytes
FROM mz_catalog.mz_clusters AS c
JOIN mz_catalog.mz_cluster_replicas AS r ON r.cluster_id = c.id
JOIN mz_catalog.mz_cluster_replica_sizes AS s ON s.size = r.size
WHERE c.name = current_setting('cluster')
