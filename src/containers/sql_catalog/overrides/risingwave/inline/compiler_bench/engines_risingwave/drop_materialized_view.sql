-- catalog-override: RisingWave compiler probes remove views with native DROP MATERIALIZED VIEW syntax.
DROP MATERIALIZED VIEW IF EXISTS {mv_name}
