-- catalog-override: engine-only: Databricks drops a Unity Catalog schema and its contents.
DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE
