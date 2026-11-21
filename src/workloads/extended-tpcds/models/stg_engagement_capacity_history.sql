SELECT
  LOWER(TRIM(engagement_path)) AS engagement_path,
  LOWER(TRIM(customer_path)) AS customer_path,
  LOWER(TRIM(relationship_type)) AS relationship_type,
  reported_units,
  TRY_CAST(allocated_units AS DOUBLE) AS allocated_units,
  effective_start
FROM ext_engagement_capacity_history
WHERE
  is_current = TRUE AND 1 = 1
