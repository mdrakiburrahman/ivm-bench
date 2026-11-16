-- catalog-override: engine-only: Databricks Enzyme drops temporary Unity Catalog views created for materialized-view planning.
DROP VIEW IF EXISTS {view}
