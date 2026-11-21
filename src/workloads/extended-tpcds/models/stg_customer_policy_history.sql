SELECT
  LOWER(TRIM(customer_path)) AS customer_path,
  NULLIF(TRIM(policy_type), '') AS policy_type,
  ingestion_time,
  effective_start
FROM ext_customer_policy_history
WHERE
  is_current = TRUE AND 1 = 1
