-- catalog-override: engine-only: Databricks OpenIVM serializes differing rows for Delta validation diagnostics.
SELECT to_json(struct(*), map('ignoreNullFields', 'false')) FROM {query} LIMIT 5
