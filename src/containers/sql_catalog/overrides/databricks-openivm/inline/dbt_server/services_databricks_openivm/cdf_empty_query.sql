-- catalog-override: engine-only: Databricks OpenIVM models empty Delta change ranges with a typed zero-row query.
SELECT {columns}, CAST(NULL AS TIMESTAMP) AS openivm_timestamp, CAST(0 AS INT) AS openivm_multiplicity FROM {table} WHERE FALSE
