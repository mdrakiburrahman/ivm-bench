-- catalog-override: engine-only: Databricks OpenIVM publishes Delta-backed model output as regular Databricks views.
CREATE VIEW {view} AS SELECT {columns} FROM {table}{where_clause}
