-- catalog-override: engine-only: Databricks Enzyme probes its Serverless SQL warehouse before timing.
SELECT 1 AS warmup_probe
