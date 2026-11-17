-- catalog-override: engine-only: Databricks Enzyme stages Delta files in a Unity Catalog volume.
CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}
