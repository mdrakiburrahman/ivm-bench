SELECT
  registry_key,
  account_id,
  LOWER(TRIM(entity_path)) AS entity_path,
  LOWER(TRIM(entity_type)) AS entity_type,
  ROW_NUMBER() OVER (PARTITION BY registry_key ORDER BY effective_start DESC) AS rn
FROM ext_entity_registry
WHERE
  1 = 1
