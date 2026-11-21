SELECT
  LOWER(TRIM(customer_path)) AS customer_path,
  TRIM(preference_sku) AS preference_sku,
  TRIM(preference_type) AS preference_type,
  ingestion_time,
  effective_start
FROM ext_customer_preference_history
WHERE
  is_current = TRUE AND 1 = 1
