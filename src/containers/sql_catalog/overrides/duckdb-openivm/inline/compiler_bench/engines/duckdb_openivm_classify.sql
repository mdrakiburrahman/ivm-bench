-- catalog-override: engine-only: DuckDB reads the OpenIVM extension catalog to classify a view.
SELECT type FROM "{catalog}".main.openivm_views WHERE view_name = '{mv_name}'
