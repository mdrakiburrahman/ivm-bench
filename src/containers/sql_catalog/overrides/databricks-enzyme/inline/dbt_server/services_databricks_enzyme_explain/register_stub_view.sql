-- catalog-override: engine-only: Databricks Enzyme registers regular Unity Catalog views so EXPLAIN can resolve downstream dependencies.
CREATE OR REPLACE VIEW {view} AS
{compiled_sql}
